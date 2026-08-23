//! C bindings over the `vortex` crate for ClickHouse's `Vortex` input and output formats. Arrays
//! cross the boundary as Arrow C Data Interface structs, and reads and writes are delegated back
//! through callbacks, so a file is always accessed through ClickHouse's own buffers - local disk,
//! S3, HTTP - with their throttling and accounting.
//!
//! Nothing here owns a thread. An `FFI_VortexRuntime` is two queues of pending work plus a way to
//! report that something became runnable; who runs it, when, and on how many threads is the
//! caller's decision, and the two queues can go to different thread pools. Without a notification
//! callback the runtime only advances on the thread already inside a call, which is enough for
//! opening a file and for writing one.
//!
//! Anything that can fail takes a `char ** error`. On failure it points at a message the caller has
//! to release with `vortex_ffi_free_string`.

// The handle and value types spell out that they live on the C boundary, the way `FFI_ArrowSchema`
// and `FFI_ArrowArray` do in the signatures right next to them.
#![allow(non_camel_case_types)]

use std::any::Any;
use std::ffi::{c_char, c_void, CStr, CString};
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

use arrow_array::cast::AsArray;
use arrow_array::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatch, StructArray};
use arrow_schema::{Field, Schema, SchemaRef};
use async_task::Runnable;
use concurrent_queue::ConcurrentQueue;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use vortex::array::buffer::BufferHandle;
use vortex::array::VortexSessionExecute;
use vortex::arrow::ArrowSessionExt;
use vortex::buffer::{Alignment, ByteBufferMut};
use vortex::dtype::{FieldName, Nullability};
use vortex::error::{vortex_err, VortexResult};
use vortex::expr::{get_item, is_null, lit, not, root, select, Expression};
use vortex::extension::datetime::{Date, TimeUnit, Timestamp, TimestampOptions};
use vortex::file::{OpenOptionsSessionExt, VortexFile, WriteOptionsSessionExt};
use vortex::io::runtime::{AbortHandle, AbortHandleRef, Executor, Handle, Task};
use vortex::io::session::RuntimeSessionExt;
use vortex::io::{CoalesceConfig, IoBuf, VortexReadAt, VortexWrite};
use vortex::scalar::Scalar;
use vortex::scalar_fn::fns::binary::Binary;
use vortex::scalar_fn::fns::operators::Operator;
use vortex::scalar_fn::ScalarFnVTableExt;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;

/// Reads `length` bytes at `offset` into `out`. Returns zero on success.
pub type FFI_VortexReadCallback =
    unsafe extern "C" fn(context: *mut c_void, offset: u64, length: u64, out: *mut u8) -> i32;

/// Consumes `length` bytes of the file being written. Returns zero on success.
pub type FFI_VortexWriteCallback =
    unsafe extern "C" fn(context: *mut c_void, data: *const u8, length: u64) -> i32;

#[repr(i32)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FFI_VortexTaskQueue {
    /// Decoding, filtering and exporting to Arrow: work that needs a core.
    CPU = 0,
    /// Work that calls the read callback and blocks until it returns.
    IO = 1,
}

const NUM_QUEUES: usize = 2;

/// Reports that a task of this queue became runnable. It must not call back into the library:
/// schedule `vortex_ffi_runtime_run` somewhere and return. Can be called on any thread, and
/// synchronously from inside any call that woke a task.
pub type FFI_VortexTaskReadyCallback =
    unsafe extern "C" fn(context: *mut c_void, queue: FFI_VortexTaskQueue);

/// The whole of the threading in this crate. Futures that Vortex spawns become `Runnable`s in one
/// of the two queues, and a `Runnable` only ever runs inside `vortex_ffi_runtime_run` or
/// `block_on`.
struct HostRuntime {
    queues: [ConcurrentQueue<Runnable>; NUM_QUEUES],
    notify: Option<FFI_VortexTaskReadyCallback>,
    /// The caller's pointer, as an integer to keep this struct `Send`.
    context: usize,
    /// Used when scheduling: work whose runtime is already gone is dropped rather than queued.
    weak_self: Weak<HostRuntime>,
    /// Threads inside `block_on` on this runtime, which have to be woken when work is queued.
    parked: Mutex<Vec<(u64, parking::Unparker)>>,
    num_parked: AtomicUsize,
    next_parked_id: AtomicU64,
}

impl HostRuntime {
    fn new(context: usize, notify: Option<FFI_VortexTaskReadyCallback>) -> Arc<Self> {
        Arc::new_cyclic(|weak_self| Self {
            queues: [ConcurrentQueue::unbounded(), ConcurrentQueue::unbounded()],
            notify,
            context,
            weak_self: weak_self.clone(),
            parked: Mutex::new(Vec::new()),
            num_parked: AtomicUsize::new(0),
            next_parked_id: AtomicU64::new(0),
        })
    }

    /// The handle Vortex spawns through. Deliberately a weak reference: the runtime is kept alive
    /// by the reader or writer that owns it.
    fn handle(self: &Arc<Self>) -> Handle {
        let executor: Arc<dyn Executor> = self.clone();
        Handle::new(Arc::downgrade(&executor))
    }

    fn spawn_on(
        &self,
        queue: FFI_VortexTaskQueue,
        future: BoxFuture<'static, ()>,
    ) -> AbortHandleRef {
        let weak = self.weak_self.clone();
        let schedule = move |runnable: Runnable| match weak.upgrade() {
            // Dropping a `Runnable` drops its future, which is what should happen to work
            // scheduled after its runtime is gone.
            None => drop(runnable),
            Some(runtime) => runtime.enqueue(queue, runnable),
        };
        // `Runnable::run` re-raises a panic of the future on the thread that ran it, and that
        // thread belongs to the caller's pool. Catch it before it gets there.
        let (runnable, task) = async_task::spawn(
            async move {
                let _ = AssertUnwindSafe(future).catch_unwind().await;
            },
            schedule,
        );
        runnable.schedule();
        Box::new(HostAbortHandle { task: Some(task) })
    }

    fn enqueue(&self, queue: FFI_VortexTaskQueue, runnable: Runnable) {
        // Unbounded and never closed, so this cannot fail.
        let _ = self.queues[queue as usize].push(runnable);
        if let Some(notify) = self.notify {
            unsafe { notify(self.context as *mut c_void, queue) };
        }
        if self.num_parked.load(Ordering::Acquire) > 0 {
            let parked = self.parked.lock().unwrap_or_else(|e| e.into_inner());
            for (_, unparker) in parked.iter() {
                unparker.unpark();
            }
        }
    }

    fn run(&self, queue: FFI_VortexTaskQueue, max_tasks: usize) -> usize {
        let mut count = 0;
        while count < max_tasks {
            match self.queues[queue as usize].pop() {
                Err(_) => break,
                Ok(runnable) => {
                    runnable.run();
                    count += 1;
                }
            }
        }
        count
    }

    fn pending(&self, queue: FFI_VortexTaskQueue) -> usize {
        self.queues[queue as usize].len()
    }

    /// Runs this runtime on the calling thread until `future` completes. This is what lets the
    /// calls that are synchronous from outside work whether or not the caller drives the queues.
    fn block_on<F: Future>(self: &Arc<Self>, future: F) -> F::Output {
        let parker = parking::Parker::new();
        let unparker = parker.unparker();
        let id = self.next_parked_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut parked = self.parked.lock().unwrap_or_else(|e| e.into_inner());
            parked.push((id, unparker.clone()));
        }
        // Published only after the unparker is in the list, so that whoever observes the count
        // also observes the unparker.
        self.num_parked.fetch_add(1, Ordering::Release);

        let waker = Waker::from(unparker);
        let mut context = Context::from_waker(&waker);
        let mut future = pin!(future);
        let output = loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                break output;
            }
            // The caller's threads may be draining the same queues; whichever gets the task runs
            // it, and the waker wakes this thread when the future can make progress.
            if self.run(FFI_VortexTaskQueue::CPU, 1) > 0 || self.run(FFI_VortexTaskQueue::IO, 1) > 0
            {
                continue;
            }
            parker.park();
        };

        self.num_parked.fetch_sub(1, Ordering::Release);
        {
            let mut parked = self.parked.lock().unwrap_or_else(|e| e.into_inner());
            parked.retain(|(parked_id, _)| *parked_id != id);
        }
        output
    }
}

impl Executor for HostRuntime {
    fn spawn(&self, future: BoxFuture<'static, ()>) -> AbortHandleRef {
        self.spawn_on(FFI_VortexTaskQueue::CPU, future)
    }

    fn spawn_io(&self, future: BoxFuture<'static, ()>) -> AbortHandleRef {
        self.spawn_on(FFI_VortexTaskQueue::IO, future)
    }

    fn spawn_cpu(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
        self.spawn_on(FFI_VortexTaskQueue::CPU, async move { task() }.boxed())
    }

    fn spawn_blocking_io(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
        self.spawn_on(FFI_VortexTaskQueue::IO, async move { task() }.boxed())
    }
}

/// Aborting drops the `async_task::Task`, which drops the future as soon as the task is idle.
/// Dropping the handle without aborting detaches the task instead, and it runs to completion.
struct HostAbortHandle {
    task: Option<async_task::Task<()>>,
}

impl AbortHandle for HostAbortHandle {
    fn abort(mut self: Box<Self>) {
        drop(self.task.take());
    }
}

impl Drop for HostAbortHandle {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.detach();
        }
    }
}

pub struct FFI_VortexRuntime {
    inner: Arc<HostRuntime>,
}

/// Creates a runtime. A null `notify`, together with `context`, gives one that only advances
/// inside FFI calls. It has to outlive every reader, scan and writer created on it.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_runtime_new(
    context: *mut c_void,
    notify: Option<FFI_VortexTaskReadyCallback>,
) -> *mut FFI_VortexRuntime {
    Box::into_raw(Box::new(FFI_VortexRuntime {
        inner: HostRuntime::new(context as usize, notify),
    }))
}

/// Runs up to `max_tasks` runnable tasks of the queue, 0 meaning no limit, and returns how many
/// were run. Returns -1 if a panic was caught; no panic ever crosses the boundary.
///
/// Any number of threads may run the same queue at once. A task may queue further tasks, on either
/// queue, which is reported through the notification callback.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_runtime_run(
    runtime: *const FFI_VortexRuntime,
    queue: FFI_VortexTaskQueue,
    max_tasks: u32,
    error: *mut *mut c_char,
) -> i64 {
    unsafe {
        ffi_wrap(error, -1, || {
            let runtime = &*runtime;
            let max_tasks = if max_tasks == 0 {
                usize::MAX
            } else {
                max_tasks as usize
            };
            Ok(runtime.inner.run(queue, max_tasks) as i64)
        })
    }
}

/// Returns the number of tasks waiting in the given queue. Safe to call from any thread.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_runtime_pending(
    runtime: *const FFI_VortexRuntime,
    queue: FFI_VortexTaskQueue,
) -> u64 {
    unsafe { (*runtime).inner.pending(queue) as u64 }
}

/// Frees the runtime. Everything created on it has to be freed first, and no thread may be inside
/// `vortex_ffi_runtime_run` on it.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_runtime_free(runtime: *mut FFI_VortexRuntime) {
    if !runtime.is_null() {
        unsafe { drop(Box::from_raw(runtime)) };
    }
}

pub struct FFI_VortexReader {
    session: VortexSession,
    runtime: Arc<HostRuntime>,
    file: VortexFile,
    schema: SchemaRef,
    /// Taken while a scan of this reader is alive. Sibling scans would share the read callback's
    /// context and would each get the reader's whole `io_concurrency`, so a reader that was
    /// downgraded to one read at a time would see several, which is exactly what it cannot take.
    scan_slot: Arc<AtomicBool>,
}

/// Holds the reader's scan slot until the scan that took it owns it.
struct ScanSlotGuard {
    slot: Arc<AtomicBool>,
    held: bool,
}

impl ScanSlotGuard {
    /// Takes the slot, or reports that a scan already has it.
    fn take(slot: &Arc<AtomicBool>) -> Result<Self, String> {
        if slot.swap(true, Ordering::AcqRel) {
            return Err("a scan of this Vortex reader is already alive".to_string());
        }
        Ok(Self {
            slot: slot.clone(),
            held: true,
        })
    }

    /// Passes the slot on to the scan, which gives it back when it is freed.
    fn into_slot(mut self) -> Arc<AtomicBool> {
        self.held = false;
        self.slot.clone()
    }
}

impl Drop for ScanSlotGuard {
    fn drop(&mut self) {
        if self.held {
            self.slot.store(false, Ordering::Release);
        }
    }
}

pub struct FFI_VortexScan {
    /// What every chunk of this scan is exported with: the file schema cut to the columns asked for.
    schema: SchemaRef,
    /// How much the scan is allowed to have in the air. One is claimed before a split is started
    /// and comes back when the caller is done with the chunk. Closing the channel calls the scan
    /// off entirely.
    permits: kanal::AsyncReceiver<()>,
    /// Starts the split tasks and announces the end. Dropping it takes them down with it.
    driver: Mutex<Option<Task<()>>>,
    /// The reader's scan slot, given back when this scan is freed.
    scan_slot: Arc<AtomicBool>,
}

impl Drop for FFI_VortexScan {
    fn drop(&mut self) {
        self.scan_slot.store(false, Ordering::Release);
    }
}

pub struct FFI_VortexExpression(Expression);

pub struct FFI_VortexWriter {
    session: VortexSession,
    runtime: Arc<HostRuntime>,
    schema: SchemaRef,
    writer: Option<vortex::file::Writer<'static>>,
}

unsafe fn set_error(error: *mut *mut c_char, message: String) {
    if error.is_null() {
        return;
    }
    let message = CString::new(message.replace('\0', " "))
        .unwrap_or_else(|_| CString::new("invalid error message").expect("valid literal"));
    unsafe {
        *error = message.into_raw();
    }
}

fn panic_message(panic: &(dyn Any + Send)) -> String {
    let message = panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string());
    format!("panic: {message}")
}

/// Runs `f` and turns anything that goes wrong, an error or a panic, into a message in `error` and
/// a return of `on_error`.
unsafe fn ffi_wrap<T, F>(error: *mut *mut c_char, on_error: T, f: F) -> T
where
    F: FnOnce() -> Result<T, String>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(value)) => value,
        Ok(Err(message)) => {
            unsafe { set_error(error, message) };
            on_error
        }
        Err(panic) => {
            unsafe { set_error(error, panic_message(panic.as_ref())) };
            on_error
        }
    }
}

/// `VortexReadAt` on top of the caller's read callback. A read is queued as IO work, so it holds
/// whichever thread picked it up for as long as the callback takes, while decoding carries on.
#[derive(Clone)]
struct CallbackReader {
    context: usize,
    read: FFI_VortexReadCallback,
    size: u64,
    concurrency: usize,
    /// When to merge neighbouring segment reads into one call. `None` asks for one call apiece.
    coalesce: Option<CoalesceConfig>,
    handle: Handle,
}

impl VortexReadAt for CallbackReader {
    fn coalesce_config(&self) -> Option<CoalesceConfig> {
        self.coalesce
    }

    fn concurrency(&self) -> usize {
        self.concurrency
    }

    fn size(&self) -> BoxFuture<'static, VortexResult<u64>> {
        let size = self.size;
        async move { Ok(size) }.boxed()
    }

    fn read_at(
        &self,
        offset: u64,
        length: usize,
        alignment: Alignment,
    ) -> BoxFuture<'static, VortexResult<BufferHandle>> {
        let this = self.clone();
        self.handle
            .spawn_blocking(move || {
                if offset
                    .checked_add(length as u64)
                    .is_none_or(|end| end > this.size)
                {
                    return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
                }
                // The callback either writes all `length` bytes or fails, so there is nothing to
                // gain from zeroing this first.
                let mut buffer = ByteBufferMut::with_capacity_aligned(length, alignment);
                let result = unsafe {
                    (this.read)(
                        this.context as *mut c_void,
                        offset,
                        length as u64,
                        buffer.spare_capacity_mut().as_mut_ptr().cast(),
                    )
                };
                if result != 0 {
                    return Err(vortex_err!("ClickHouse read callback failed"));
                }
                unsafe { buffer.set_len(length) };
                Ok(BufferHandle::new_host(buffer.freeze()))
            })
            .boxed()
    }
}

fn make_session(runtime: &Arc<HostRuntime>) -> VortexSession {
    VortexSession::default().with_handle(runtime.handle())
}

/// Zero-initialize for one read at a time and no merging.
#[repr(C)]
pub struct FFI_VortexReaderOptions {
    /// How many reads may be outstanding, 0 and 1 both meaning one. Above that the read callback
    /// has to tolerate being on several threads at once.
    pub io_concurrency: u32,
    /// Two segment reads are merged into one call when no more than `coalesce_max_gap_bytes`
    /// separate them and the result stays under `coalesce_max_read_bytes`. Both zero disables
    /// merging.
    pub coalesce_max_gap_bytes: u64,
    pub coalesce_max_read_bytes: u64,
}

/// Opens a file for reading. It is accessed through `read` with the given `context`; `file_size`
/// has to be the exact size of the file, and `options` may be null. Reading the footer happens on
/// the calling thread, so the runtime does not have to be driven yet. Returns null on failure.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_reader_open(
    runtime: *const FFI_VortexRuntime,
    context: *mut c_void,
    read: FFI_VortexReadCallback,
    file_size: u64,
    options: *const FFI_VortexReaderOptions,
    error: *mut *mut c_char,
) -> *mut FFI_VortexReader {
    unsafe {
        ffi_wrap(error, std::ptr::null_mut(), || {
            let runtime = (*runtime).inner.clone();
            let mut concurrency = 1usize;
            let mut coalesce = None;
            if !options.is_null() {
                let options = &*options;
                concurrency = std::cmp::max(options.io_concurrency, 1) as usize;
                if options.coalesce_max_gap_bytes != 0 || options.coalesce_max_read_bytes != 0 {
                    coalesce = Some(CoalesceConfig::new(
                        options.coalesce_max_gap_bytes,
                        options.coalesce_max_read_bytes,
                    ));
                }
            }
            let session = make_session(&runtime);
            let source = CallbackReader {
                context: context as usize,
                read,
                size: file_size,
                concurrency,
                coalesce,
                handle: runtime.handle(),
            };
            let file = runtime
                .block_on(
                    session
                        .open_options()
                        .with_file_size(file_size)
                        .open_read(source),
                )
                .map_err(|e| e.to_string())?;
            let schema = Arc::new(
                session
                    .arrow()
                    .to_arrow_schema(file.dtype())
                    .map_err(|e| e.to_string())?,
            );
            Ok(Box::into_raw(Box::new(FFI_VortexReader {
                session,
                runtime,
                file,
                schema,
                scan_slot: Arc::new(AtomicBool::new(false)),
            })))
        })
    }
}

/// Returns the total number of rows in the file.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_reader_row_count(reader: *const FFI_VortexReader) -> u64 {
    unsafe { (*reader).file.row_count() }
}

/// Exports the file schema into `out_schema`, which the caller then owns and has to release.
/// Returns zero on success.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_reader_schema(
    reader: *const FFI_VortexReader,
    out_schema: *mut FFI_ArrowSchema,
    error: *mut *mut c_char,
) -> i32 {
    unsafe {
        ffi_wrap(error, -1, || {
            let reader = &*reader;
            let ffi_schema =
                FFI_ArrowSchema::try_from(reader.schema.as_ref()).map_err(|e| e.to_string())?;
            std::ptr::write(out_schema, ffi_schema);
            Ok(0)
        })
    }
}

/// Frees the reader. Every scan created on it has to be freed first.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_reader_free(reader: *mut FFI_VortexReader) {
    if !reader.is_null() {
        unsafe { drop(Box::from_raw(reader)) };
    }
}

/// A `VortexWrite` on top of the caller's write callback. Writing never leaves the calling thread,
/// so the callback only ever runs from inside an FFI call.
struct CallbackWriter {
    context: usize,
    write: FFI_VortexWriteCallback,
}

impl VortexWrite for CallbackWriter {
    fn write_all<B: IoBuf>(
        &mut self,
        buffer: B,
    ) -> impl std::future::Future<Output = std::io::Result<B>> {
        let slice = buffer.as_slice();
        let result = unsafe {
            (self.write)(
                self.context as *mut c_void,
                slice.as_ptr(),
                slice.len() as u64,
            )
        };
        std::future::ready(if result == 0 {
            Ok(buffer)
        } else {
            Err(std::io::Error::other("ClickHouse write callback failed"))
        })
    }

    fn flush(&mut self) -> impl std::future::Future<Output = std::io::Result<()>> {
        std::future::ready(Ok(()))
    }

    fn shutdown(&mut self) -> impl std::future::Future<Output = std::io::Result<()>> {
        std::future::ready(Ok(()))
    }
}

/// Nothing here is required: zero-initialize to read every row of every column.
#[repr(C)]
pub struct FFI_VortexScanOptions {
    /// The top-level columns to read, in this order. Null means all of them.
    pub columns: *const *const c_char,
    pub num_columns: u64,
    /// Only the rows matching it are returned, and the scan skips the statistics zones it rules
    /// out. Null means no filter.
    pub filter: *const FFI_VortexExpression,
    /// The row range `[row_range_begin, row_range_end)`. Both zero means the whole file.
    pub row_range_begin: u64,
    pub row_range_end: u64,
    /// The number of splits that may be in flight at once: being read, being decoded, or already
    /// handed over and not yet released. 0 selects the default. This is what keeps the scan from
    /// running ahead of the caller; the reads underneath are bounded separately by
    /// `io_concurrency` and `coalesce_max_read_bytes`.
    pub max_splits_in_flight: u32,
}

/// The callbacks a scan reports to. Both run on the caller's own threads, possibly several at a
/// time. The only calls back into the library they may make are `vortex_ffi_scan_cancel`, which is
/// allowed from either of them, and `vortex_ffi_scan_release`, which is not allowed from
/// `on_chunk`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FFI_VortexScanCallbacks {
    pub context: *mut c_void,
    /// Delivers one chunk: an Arrow struct array in the scan's schema, together with the position
    /// of its split in the file. The array is borrowed for the duration of the call - the callback
    /// takes the data out of it (or releases it) before returning, and must not keep the pointer.
    /// A null array means the split matched no rows; it is still reported so that the caller can
    /// restore the file order. Returning non-zero stops the scan and surfaces from `on_finish` as
    /// an error.
    pub on_chunk: unsafe extern "C" fn(
        context: *mut c_void,
        array: *mut FFI_ArrowArray,
        split_index: u64,
    ) -> i32,
    /// Reports the end of the scan, exactly once: null if every split was delivered, otherwise a
    /// message that is only valid for the duration of the call. Never called for a scan that was
    /// cancelled. After a failure a split task already in flight can still reach `on_chunk`, so the
    /// context has to outlive the caller's last pass over the queues.
    pub on_finish: unsafe extern "C" fn(context: *mut c_void, error: *const c_char),
}

/// The callbacks as the tasks carry them: the pointer is kept as an integer so that the struct
/// stays `Send`. The caller guarantees the context outlives the scan.
#[derive(Clone, Copy)]
struct ScanCallbacks {
    context: usize,
    on_chunk: unsafe extern "C" fn(*mut c_void, *mut FFI_ArrowArray, u64) -> i32,
    on_finish: unsafe extern "C" fn(*mut c_void, *const c_char),
}

impl ScanCallbacks {
    fn deliver(&self, array: Option<FFI_ArrowArray>, split_index: u64) -> VortexResult<()> {
        let empty = array.is_none();
        let result = match array {
            None => unsafe {
                (self.on_chunk)(
                    self.context as *mut c_void,
                    std::ptr::null_mut(),
                    split_index,
                )
            },
            Some(array) => {
                // The array lives on this stack frame for the duration of the call: the callback
                // consumes it - imports it, which moves the data out and marks the struct released,
                // or releases it - and does not keep the pointer. It must not be released here as
                // well, so the local is a `ManuallyDrop`.
                let mut array = std::mem::ManuallyDrop::new(array);
                unsafe { (self.on_chunk)(self.context as *mut c_void, &mut *array, split_index) }
            }
        };
        if result != 0 {
            return Err(if empty {
                vortex_err!("ClickHouse rejected the empty split {split_index}")
            } else {
                vortex_err!("ClickHouse failed to convert the chunk of split {split_index}")
            });
        }
        Ok(())
    }

    fn finish(&self, error: Option<String>) {
        match error {
            None => unsafe { (self.on_finish)(self.context as *mut c_void, std::ptr::null()) },
            Some(message) => {
                let message = CString::new(message.replace('\0', " ")).unwrap_or_else(|_| {
                    CString::new("invalid error message").expect("valid literal")
                });
                unsafe { (self.on_finish)(self.context as *mut c_void, message.as_ptr()) };
            }
        }
    }
}

/// Reports the end of a scan exactly once, including when the driver panics: the caller waits for
/// `on_finish` and would otherwise wait forever. A driver that was cancelled reports nothing, which
/// is what cancellation guarantees.
struct FinishGuard {
    callbacks: ScanCallbacks,
    finished: bool,
}

impl FinishGuard {
    fn finish(mut self, error: Option<String>) {
        self.finished = true;
        self.callbacks.finish(error);
    }
}

impl Drop for FinishGuard {
    fn drop(&mut self) {
        // A panicking driver drops its locals while unwinding out of `poll`, before the runtime
        // catches the panic, so this is how a panic is distinguished from a cancellation.
        if !self.finished && std::thread::panicking() {
            self.callbacks
                .finish(Some("panic in the scan driver".to_string()));
        }
    }
}

const DEFAULT_MAX_SPLITS_IN_FLIGHT: usize = 4;

/// The error of a joined split task, if it failed or panicked.
fn scan_task_error(outcome: Result<VortexResult<()>, Box<dyn Any + Send>>) -> Option<String> {
    match outcome {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(e.to_string()),
        Err(panic) => Some(panic_message(panic.as_ref())),
    }
}

/// Creates a scan and starts it. Optimizing the expression and computing the splits happens here,
/// on the calling thread; from then on the scan is a driver task that spawns one task per split, as
/// far ahead as the `max_splits_in_flight` permits allow. A split task reads, decodes and exports
/// its data, then calls `on_chunk` on the thread it is running on, and the permit it took is only
/// returned when the caller releases that chunk - so the caller sets the pace.
///
/// A split that matched no rows returns its own permit, as there is nothing for the caller to
/// release. The driver joins finished tasks even while it is waiting for a permit: with every
/// permit held by the caller, a split that failed would otherwise never be noticed, having
/// delivered nothing.
///
/// The reader and the callbacks' context both have to outlive the scan. `filter` is borrowed, not
/// consumed. Returns null on failure.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_scan_create(
    reader: *const FFI_VortexReader,
    options: *const FFI_VortexScanOptions,
    callbacks: *const FFI_VortexScanCallbacks,
    error: *mut *mut c_char,
) -> *mut FFI_VortexScan {
    unsafe {
        ffi_wrap(error, std::ptr::null_mut(), || {
            let reader = &*reader;
            // One scan per reader: everything that bounds the reads - the concurrency the read
            // callback was promised above all - is set up per scan, so a second one would double it.
            let scan_slot = ScanSlotGuard::take(&reader.scan_slot)?;
            let mut builder = reader.file.scan().map_err(|e| e.to_string())?;

            let mut schema = reader.schema.clone();
            let mut max_splits_in_flight = DEFAULT_MAX_SPLITS_IN_FLIGHT;

            if !options.is_null() {
                let options = &*options;

                if !options.columns.is_null() {
                    let mut names = Vec::with_capacity(options.num_columns as usize);
                    for i in 0..options.num_columns {
                        let name = CStr::from_ptr(*options.columns.add(i as usize))
                            .to_str()
                            .map_err(|e| e.to_string())?;
                        names.push(name.to_string());
                    }
                    let mut fields = Vec::with_capacity(names.len());
                    for name in &names {
                        fields.push(
                            reader
                                .schema
                                .field_with_name(name)
                                .map_err(|e| e.to_string())?
                                .clone(),
                        );
                    }
                    let field_names: Vec<FieldName> = names
                        .iter()
                        .map(|name| FieldName::from(name.as_str()))
                        .collect();
                    // `with_projection` and `with_filter` take an expression already bound to
                    // the file's type, which is where a column that is not in the file or a
                    // comparison of two types that cannot be compared is now caught.
                    builder = builder.with_projection(
                        select(field_names, root())
                            .bind(reader.file.dtype())
                            .map_err(|e| e.to_string())?,
                    );
                    schema = Arc::new(Schema::new(fields));
                }

                if !options.filter.is_null() {
                    builder = builder.with_filter(
                        (*options.filter)
                            .0
                            .bind(reader.file.dtype())
                            .map_err(|e| e.to_string())?,
                    );
                }

                if options.row_range_begin != 0 || options.row_range_end != 0 {
                    if options.row_range_begin > options.row_range_end {
                        return Err(format!(
                            "invalid row range [{}, {})",
                            options.row_range_begin, options.row_range_end
                        ));
                    }
                    builder =
                        builder.with_row_range(options.row_range_begin..options.row_range_end);
                }

                if options.max_splits_in_flight != 0 {
                    max_splits_in_flight = options.max_splits_in_flight as usize;
                }
            }

            let callbacks = {
                let callbacks = &*callbacks;
                ScanCallbacks {
                    context: callbacks.context as usize,
                    on_chunk: callbacks.on_chunk,
                    on_finish: callbacks.on_finish,
                }
            };

            // The export to Arrow happens inside the split task, on the caller's thread, and is
            // checked against the scan schema so that every chunk can be imported with the single
            // schema `vortex_ffi_scan_schema` returns.
            let session = reader.session.clone();
            let struct_field = Field::new_struct("", schema.fields().clone(), false);
            let expected_type = struct_field.data_type().clone();
            let builder = builder.map(move |chunk| {
                let mut ctx = session.create_execution_ctx();
                let arrow = session
                    .arrow()
                    .execute_arrow(chunk, Some(&struct_field), &mut ctx)?;
                if arrow.data_type() != &expected_type {
                    return Err(vortex_err!(
                        "Vortex chunk exported as {} instead of the scan schema {}",
                        arrow.data_type(),
                        expected_type
                    ));
                }
                Ok(FFI_ArrowArray::new(&arrow.as_struct().to_data()))
            });

            // `into_stream` and `into_iter` are on offer and are not what we want: they give back
            // a `Stream` that somebody would have to keep polling, pick their own concurrency, and
            // quietly swallow the splits that kept no rows. We need the chunks pushed instead,
            // held to `max_splits_in_flight`, and each labelled with the split it came from. `prepare` and
            // `execute` are just computation with no IO in them, so they belong right here.
            let tasks = builder
                .prepare()
                .and_then(|scan| scan.execute(None))
                .map_err(|e| e.to_string())?;

            let max_splits_in_flight = std::cmp::max(max_splits_in_flight, 1);
            // Closing this channel is what cancels the scan.
            let (permit_sender, permits) = kanal::bounded_async::<()>(max_splits_in_flight);

            let handle = reader.runtime.handle();
            let spawner = handle.clone();
            let permits_for_tasks = permits.clone();

            let driver = handle.spawn(async move {
                let mut spawned = futures::stream::FuturesUnordered::new();
                let mut error: Option<String> = None;
                let guard = FinishGuard {
                    callbacks,
                    finished: false,
                };

                for (split_index, task) in tasks.into_iter().enumerate() {
                    // Joining while waiting for a permit is what lets a failed split stop the
                    // scan when the caller is holding all of them.
                    let mut send = pin!(permit_sender.send(()).fuse());
                    loop {
                        futures::select_biased! {
                            outcome = spawned.select_next_some() => {
                                error = scan_task_error(outcome);
                                if error.is_some() {
                                    break;
                                }
                            }
                            result = send => {
                                // Only fails once the scan is cancelled, and then there is
                                // nobody left to report to.
                                if result.is_err() {
                                    return;
                                }
                                break;
                            }
                        }
                    }
                    if error.is_some() {
                        break;
                    }

                    let split_index = split_index as u64;
                    let permits = permits_for_tasks.clone();
                    let task = spawner.spawn(async move {
                        match task.await {
                            Ok(Some(array)) => callbacks.deliver(Some(array), split_index),
                            Ok(None) => {
                                // Nothing was delivered, so the permit is returned here.
                                let result = callbacks.deliver(None, split_index);
                                let _ = permits.try_recv();
                                result
                            }
                            Err(e) => Err(e),
                        }
                    });
                    // Joining a panicking task re-raises the panic; catching it here turns it
                    // into an error the caller can be told about.
                    spawned.push(AssertUnwindSafe(task).catch_unwind());
                }

                while error.is_none() {
                    match spawned.next().await {
                        None => break,
                        Some(outcome) => error = scan_task_error(outcome),
                    }
                }

                // Cancel whatever is still running after a failure, before reporting the end.
                drop(spawned);
                guard.finish(error);
            });

            Ok(Box::into_raw(Box::new(FFI_VortexScan {
                schema,
                permits,
                driver: Mutex::new(Some(driver)),
                scan_slot: scan_slot.into_slot(),
            })))
        })
    }
}

/// Exports the schema of the scan's chunks into `out_schema`, which the caller then owns and has
/// to release. Returns zero on success.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_scan_schema(
    scan: *const FFI_VortexScan,
    out_schema: *mut FFI_ArrowSchema,
    error: *mut *mut c_char,
) -> i32 {
    unsafe {
        ffi_wrap(error, -1, || {
            let scan = &*scan;
            let ffi_schema =
                FFI_ArrowSchema::try_from(scan.schema.as_ref()).map_err(|e| e.to_string())?;
            std::ptr::write(out_schema, ffi_schema);
            Ok(0)
        })
    }
}

/// Returns the capacity taken by `count` chunks the caller has finished with, letting the scan
/// read that many splits further ahead. Safe from any thread and a no-op once the scan has ended.
/// Must not be called from inside `on_chunk`.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_scan_release(scan: *const FFI_VortexScan, count: u64) {
    if scan.is_null() {
        return;
    }
    let scan = unsafe { &*scan };
    for _ in 0..count {
        if scan.permits.try_recv().is_err() {
            break;
        }
    }
}

/// Cancels the scan; safe from any thread. Pending tasks are dropped and no callback happens after
/// this returns, except from a task that was already running - so stop driving the queues before
/// releasing the callbacks' context.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_scan_cancel(scan: *const FFI_VortexScan) {
    if scan.is_null() {
        return;
    }
    let scan = unsafe { &*scan };
    // Stops the driver from spawning any more split tasks.
    let _ = scan.permits.close();
    // Dropping the driver task cancels it, together with the split tasks it owns.
    let driver = scan.driver.lock().unwrap_or_else(|e| e.into_inner()).take();
    drop(driver);
}

/// Frees the scan. The queues must no longer be driven: no task of it may still be running.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_scan_free(scan: *mut FFI_VortexScan) {
    if !scan.is_null() {
        unsafe { drop(Box::from_raw(scan)) };
    }
}

/// The type of a literal. It has to be exactly the type of the file column it is compared with:
/// Vortex requires both sides of a comparison to have the same type.
#[repr(i32)]
#[derive(Clone, Copy)]
pub enum FFI_VortexPrimitiveType {
    I8 = 0,
    I16 = 1,
    I32 = 2,
    I64 = 3,
    U8 = 4,
    U16 = 5,
    U32 = 6,
    U64 = 7,
    F32 = 8,
    F64 = 9,
}

#[repr(i32)]
#[derive(Clone, Copy)]
pub enum FFI_VortexComparisonOperator {
    Eq = 0,
    NotEq = 1,
    Lt = 2,
    Lte = 3,
    Gt = 4,
    Gte = 5,
}

/// The unit of a temporal literal; the values mirror the discriminants of the Vortex `TimeUnit`.
#[repr(i32)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FFI_VortexTimeUnit {
    Nanoseconds = 0,
    Microseconds = 1,
    Milliseconds = 2,
    Seconds = 3,
    Days = 4,
}

impl From<FFI_VortexTimeUnit> for TimeUnit {
    fn from(unit: FFI_VortexTimeUnit) -> TimeUnit {
        match unit {
            FFI_VortexTimeUnit::Nanoseconds => TimeUnit::Nanoseconds,
            FFI_VortexTimeUnit::Microseconds => TimeUnit::Microseconds,
            FFI_VortexTimeUnit::Milliseconds => TimeUnit::Milliseconds,
            FFI_VortexTimeUnit::Seconds => TimeUnit::Seconds,
            FFI_VortexTimeUnit::Days => TimeUnit::Days,
        }
    }
}

// Every builder below returns null for input it cannot use, borrows rather than consumes its
// arguments, and returns a handle that has to be freed with `vortex_ffi_expr_free`.

/// Creates an expression referencing the top-level column `name`.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_column(name: *const c_char) -> *mut FFI_VortexExpression {
    if name.is_null() {
        return std::ptr::null_mut();
    }
    let Ok(name) = (unsafe { CStr::from_ptr(name) }).to_str() else {
        return std::ptr::null_mut();
    };
    let expr = get_item(FieldName::from(name), root());
    Box::into_raw(Box::new(FFI_VortexExpression(expr)))
}

/// Creates a signed integer literal of the given type. Returns null if the value does not fit.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_int(
    ptype: FFI_VortexPrimitiveType,
    value: i64,
) -> *mut FFI_VortexExpression {
    let nullability = Nullability::NonNullable;
    let scalar = match ptype {
        FFI_VortexPrimitiveType::I8 => match i8::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        FFI_VortexPrimitiveType::I16 => match i16::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        FFI_VortexPrimitiveType::I32 => match i32::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        FFI_VortexPrimitiveType::I64 => Scalar::primitive(value, nullability),
        _ => return std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(FFI_VortexExpression(lit(scalar))))
}

/// Creates an unsigned integer literal of the given type. Returns null if the value does not fit.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_uint(
    ptype: FFI_VortexPrimitiveType,
    value: u64,
) -> *mut FFI_VortexExpression {
    let nullability = Nullability::NonNullable;
    let scalar = match ptype {
        FFI_VortexPrimitiveType::U8 => match u8::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        FFI_VortexPrimitiveType::U16 => match u16::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        FFI_VortexPrimitiveType::U32 => match u32::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        FFI_VortexPrimitiveType::U64 => Scalar::primitive(value, nullability),
        _ => return std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(FFI_VortexExpression(lit(scalar))))
}

/// Creates a floating-point literal of the given type. An `F32` value has to be exactly
/// representable as `f32`; a rounded bound would change which rows the comparison matches.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_float(
    ptype: FFI_VortexPrimitiveType,
    value: f64,
) -> *mut FFI_VortexExpression {
    let nullability = Nullability::NonNullable;
    let scalar = match ptype {
        FFI_VortexPrimitiveType::F32 => {
            let narrowed = value as f32;
            if f64::from(narrowed) != value {
                return std::ptr::null_mut();
            }
            Scalar::primitive(narrowed, nullability)
        }
        FFI_VortexPrimitiveType::F64 => Scalar::primitive(value, nullability),
        _ => return std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(FFI_VortexExpression(lit(scalar))))
}

/// Creates a boolean literal.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_bool(value: bool) -> *mut FFI_VortexExpression {
    let scalar = Scalar::bool(value, Nullability::NonNullable);
    Box::into_raw(Box::new(FFI_VortexExpression(lit(scalar))))
}

/// Creates a string literal. `is_utf8` selects a `Utf8` literal, whose bytes have to be valid
/// UTF-8, or a `Binary` one. A null `data` is only accepted for length 0.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_string(
    data: *const u8,
    length: u64,
    is_utf8: bool,
) -> *mut FFI_VortexExpression {
    // `from_raw_parts` requires a non-null, aligned pointer even for an empty slice.
    let bytes: &[u8] = if data.is_null() {
        if length != 0 {
            return std::ptr::null_mut();
        }
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(data, length as usize) }
    };
    let scalar = if is_utf8 {
        let Ok(string) = std::str::from_utf8(bytes) else {
            return std::ptr::null_mut();
        };
        Scalar::utf8(string.to_string(), Nullability::NonNullable)
    } else {
        Scalar::binary(bytes.to_vec(), Nullability::NonNullable)
    };
    Box::into_raw(Box::new(FFI_VortexExpression(lit(scalar))))
}

/// Creates a `vortex.date` literal: days or milliseconds since the Unix epoch. The only units a
/// date supports are `Days`, whose value has to fit `i32`, and `Milliseconds`. Returns null
/// otherwise.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_date(
    unit: FFI_VortexTimeUnit,
    value: i64,
) -> *mut FFI_VortexExpression {
    let storage = match unit {
        FFI_VortexTimeUnit::Days => match i32::try_from(value) {
            Ok(days) => Scalar::primitive(days, Nullability::NonNullable),
            Err(_) => return std::ptr::null_mut(),
        },
        FFI_VortexTimeUnit::Milliseconds => Scalar::primitive(value, Nullability::NonNullable),
        _ => return std::ptr::null_mut(),
    };
    let scalar = Scalar::extension::<Date>(TimeUnit::from(unit), storage);
    Box::into_raw(Box::new(FFI_VortexExpression(lit(scalar))))
}

/// Creates a `vortex.timestamp` literal: ticks of `unit` since the Unix epoch, with `timezone`
/// naming the zone or null for a zone-less timestamp. `Days` is not a timestamp unit. The unit and
/// the zone have to be exactly the file column's: Vortex only compares timestamps whose metadata
/// is identical.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_timestamp(
    unit: FFI_VortexTimeUnit,
    timezone: *const c_char,
    value: i64,
) -> *mut FFI_VortexExpression {
    if unit == FFI_VortexTimeUnit::Days {
        return std::ptr::null_mut();
    }
    let tz = if timezone.is_null() {
        None
    } else {
        match (unsafe { CStr::from_ptr(timezone) }).to_str() {
            Ok(name) => Some(std::sync::Arc::<str>::from(name)),
            Err(_) => return std::ptr::null_mut(),
        }
    };
    let options = TimestampOptions {
        unit: TimeUnit::from(unit),
        tz,
    };
    let storage = Scalar::primitive(value, Nullability::NonNullable);
    let scalar = Scalar::extension::<Timestamp>(options, storage);
    Box::into_raw(Box::new(FFI_VortexExpression(lit(scalar))))
}

/// Creates a comparison `lhs op rhs`. A comparison with a null value yields null, which the scan
/// treats as a row that does not match.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_compare(
    comparison: FFI_VortexComparisonOperator,
    lhs: *const FFI_VortexExpression,
    rhs: *const FFI_VortexExpression,
) -> *mut FFI_VortexExpression {
    if lhs.is_null() || rhs.is_null() {
        return std::ptr::null_mut();
    }
    let operator = match comparison {
        FFI_VortexComparisonOperator::Eq => Operator::Eq,
        FFI_VortexComparisonOperator::NotEq => Operator::NotEq,
        FFI_VortexComparisonOperator::Lt => Operator::Lt,
        FFI_VortexComparisonOperator::Lte => Operator::Lte,
        FFI_VortexComparisonOperator::Gt => Operator::Gt,
        FFI_VortexComparisonOperator::Gte => Operator::Gte,
    };
    let expr = unsafe { Binary.new_expr(operator, [(*lhs).0.clone(), (*rhs).0.clone()]) };
    Box::into_raw(Box::new(FFI_VortexExpression(expr)))
}

/// Creates a Kleene, three-valued AND of two boolean expressions.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_and(
    lhs: *const FFI_VortexExpression,
    rhs: *const FFI_VortexExpression,
) -> *mut FFI_VortexExpression {
    if lhs.is_null() || rhs.is_null() {
        return std::ptr::null_mut();
    }
    let expr = unsafe { Binary.new_expr(Operator::And, [(*lhs).0.clone(), (*rhs).0.clone()]) };
    Box::into_raw(Box::new(FFI_VortexExpression(expr)))
}

/// Creates a Kleene, three-valued OR of two boolean expressions.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_or(
    lhs: *const FFI_VortexExpression,
    rhs: *const FFI_VortexExpression,
) -> *mut FFI_VortexExpression {
    if lhs.is_null() || rhs.is_null() {
        return std::ptr::null_mut();
    }
    let expr = unsafe { Binary.new_expr(Operator::Or, [(*lhs).0.clone(), (*rhs).0.clone()]) };
    Box::into_raw(Box::new(FFI_VortexExpression(expr)))
}

/// Creates a logical NOT of a boolean expression. NOT of a null is null.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_not(
    child: *const FFI_VortexExpression,
) -> *mut FFI_VortexExpression {
    if child.is_null() {
        return std::ptr::null_mut();
    }
    let expr = unsafe { not((*child).0.clone()) };
    Box::into_raw(Box::new(FFI_VortexExpression(expr)))
}

/// Creates an expression that is true for the rows where the child expression is null.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_is_null(
    child: *const FFI_VortexExpression,
) -> *mut FFI_VortexExpression {
    if child.is_null() {
        return std::ptr::null_mut();
    }
    let expr = unsafe { is_null((*child).0.clone()) };
    Box::into_raw(Box::new(FFI_VortexExpression(expr)))
}

/// Renders the expression the way the library prints it, for logs and error messages. The string
/// has to be freed with `vortex_ffi_free_string`.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_display(expr: *const FFI_VortexExpression) -> *mut c_char {
    if expr.is_null() {
        return std::ptr::null_mut();
    }
    let rendered = format!("{}", unsafe { &(*expr).0 });
    let rendered = CString::new(rendered.replace('\0', " "))
        .unwrap_or_else(|_| CString::new("invalid expression string").expect("valid literal"));
    rendered.into_raw()
}

/// Frees an expression handle.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_free(expr: *mut FFI_VortexExpression) {
    if !expr.is_null() {
        unsafe { drop(Box::from_raw(expr)) };
    }
}

/// Creates a writer for a file with the given schema, which it consumes. The bytes are sent to
/// `write` with the given `context`. It drives a runtime of its own on the calling thread, so
/// writing needs no threads from the caller. Returns null on failure.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_writer_create(
    context: *mut c_void,
    write: FFI_VortexWriteCallback,
    schema: *mut FFI_ArrowSchema,
    error: *mut *mut c_char,
) -> *mut FFI_VortexWriter {
    unsafe {
        ffi_wrap(error, std::ptr::null_mut(), || {
            let ffi_schema = std::ptr::read(schema);
            let arrow_schema = Schema::try_from(&ffi_schema).map_err(|e| e.to_string())?;
            let runtime = HostRuntime::new(0, None);
            let session = make_session(&runtime);
            let dtype = session
                .arrow()
                .from_arrow_schema(&arrow_schema)
                .map_err(|e| e.to_string())?;
            let sink = CallbackWriter {
                context: context as usize,
                write,
            };
            let writer = session.write_options().writer(sink, dtype);
            Ok(Box::into_raw(Box::new(FFI_VortexWriter {
                session,
                runtime,
                schema: Arc::new(arrow_schema),
                writer: Some(writer),
            })))
        })
    }
}

/// Appends one record batch, which it consumes, in the schema the writer was created with.
/// Returns zero on success.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_writer_write(
    writer: *mut FFI_VortexWriter,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    error: *mut *mut c_char,
) -> i32 {
    unsafe {
        ffi_wrap(error, -1, || {
            let writer = &mut *writer;
            let ffi_array = std::ptr::read(array);
            let ffi_schema = std::ptr::read(schema);
            let data = from_ffi(ffi_array, &ffi_schema).map_err(|e| e.to_string())?;
            let batch = RecordBatch::from(StructArray::from(data));
            let chunk = writer
                .session
                .arrow()
                .from_arrow_record_batch(batch, &writer.schema)
                .map_err(|e| e.to_string())?;
            let vortex_writer = writer
                .writer
                .as_mut()
                .ok_or_else(|| "writer is already finished".to_string())?;
            writer
                .runtime
                .block_on(vortex_writer.push(chunk))
                .map_err(|e| e.to_string())?;
            Ok(0)
        })
    }
}

/// Flushes the remaining data and writes the file footer. Must be called exactly once, before
/// freeing the writer. Returns zero on success.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_writer_finish(
    writer: *mut FFI_VortexWriter,
    error: *mut *mut c_char,
) -> i32 {
    unsafe {
        ffi_wrap(error, -1, || {
            let writer = &mut *writer;
            let vortex_writer = writer
                .writer
                .take()
                .ok_or_else(|| "writer is already finished".to_string())?;
            writer
                .runtime
                .block_on(vortex_writer.finish())
                .map_err(|e| e.to_string())?;
            Ok(0)
        })
    }
}

/// Frees the writer. Without a preceding `vortex_ffi_writer_finish` the file is left incomplete.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_writer_free(writer: *mut FFI_VortexWriter) {
    if !writer.is_null() {
        unsafe { drop(Box::from_raw(writer)) };
    }
}

/// Frees a string returned by this library, such as an error message.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_free_string(string: *mut c_char) {
    if !string.is_null() {
        unsafe { drop(CString::from_raw(string)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::ffi::to_ffi;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use std::sync::atomic::AtomicBool;
    use std::sync::Condvar;

    unsafe extern "C" fn write_to_vec(context: *mut c_void, data: *const u8, length: u64) -> i32 {
        let out = unsafe { &mut *(context as *mut Vec<u8>) };
        out.extend_from_slice(unsafe { std::slice::from_raw_parts(data, length as usize) });
        0
    }

    /// A file in memory that keeps count of how often the read callback was entered.
    struct TestFile {
        data: Vec<u8>,
        reads: AtomicUsize,
        /// Turns every read into a failure.
        fail_reads: AtomicBool,
    }

    impl TestFile {
        fn new(data: Vec<u8>) -> Self {
            Self {
                data,
                reads: AtomicUsize::new(0),
                fail_reads: AtomicBool::new(false),
            }
        }

        fn context(&mut self) -> *mut c_void {
            self as *mut TestFile as *mut c_void
        }

        fn reads(&self) -> usize {
            self.reads.load(Ordering::Relaxed)
        }
    }

    unsafe extern "C" fn read_from_vec(
        context: *mut c_void,
        offset: u64,
        length: u64,
        out: *mut u8,
    ) -> i32 {
        let file = unsafe { &*(context as *const TestFile) };
        file.reads.fetch_add(1, Ordering::Relaxed);
        if file.fail_reads.load(Ordering::Relaxed) {
            return 1;
        }
        let Some(end) = offset.checked_add(length) else {
            return 1;
        };
        if end > file.data.len() as u64 {
            return 1;
        }
        unsafe {
            std::ptr::copy_nonoverlapping(
                file.data.as_ptr().add(offset as usize),
                out,
                length as usize,
            )
        };
        0
    }

    /// Stands in for ClickHouse: worker threads that drain the queues whenever notified.
    struct TestHost {
        runtime: *mut FFI_VortexRuntime,
        state: Arc<TestHostState>,
        workers: Vec<std::thread::JoinHandle<()>>,
    }

    struct TestHostState {
        runtime: AtomicUsize,
        mutex: Mutex<bool>,
        condvar: Condvar,
        stop: AtomicBool,
        panicked: AtomicBool,
    }

    unsafe extern "C" fn test_notify(context: *mut c_void, _queue: FFI_VortexTaskQueue) {
        let state = unsafe { &*(context as *const TestHostState) };
        {
            let mut ready = state.mutex.lock().unwrap_or_else(|e| e.into_inner());
            *ready = true;
        }
        state.condvar.notify_all();
    }

    impl TestHost {
        fn new(num_workers: usize) -> Self {
            let state = Arc::new(TestHostState {
                runtime: AtomicUsize::new(0),
                mutex: Mutex::new(false),
                condvar: Condvar::new(),
                stop: AtomicBool::new(false),
                panicked: AtomicBool::new(false),
            });
            let context = Arc::as_ptr(&state) as *mut c_void;
            let runtime = unsafe { vortex_ffi_runtime_new(context, Some(test_notify)) };
            state.runtime.store(runtime as usize, Ordering::Release);

            let workers = (0..num_workers)
                .map(|_| {
                    let state = Arc::clone(&state);
                    std::thread::spawn(move || {
                        let runtime =
                            state.runtime.load(Ordering::Acquire) as *const FFI_VortexRuntime;
                        while !state.stop.load(Ordering::Relaxed) {
                            let mut error: *mut c_char = std::ptr::null_mut();
                            let cpu = unsafe {
                                vortex_ffi_runtime_run(
                                    runtime,
                                    FFI_VortexTaskQueue::CPU,
                                    8,
                                    &mut error,
                                )
                            };
                            let io = unsafe {
                                vortex_ffi_runtime_run(
                                    runtime,
                                    FFI_VortexTaskQueue::IO,
                                    8,
                                    &mut error,
                                )
                            };
                            if cpu < 0 || io < 0 {
                                state.panicked.store(true, Ordering::Relaxed);
                                if !error.is_null() {
                                    unsafe { vortex_ffi_free_string(error) };
                                }
                                continue;
                            }
                            if cpu > 0 || io > 0 {
                                continue;
                            }
                            let ready = state.mutex.lock().unwrap_or_else(|e| e.into_inner());
                            let _unused = state
                                .condvar
                                .wait_timeout(ready, std::time::Duration::from_millis(5))
                                .unwrap_or_else(|e| e.into_inner());
                        }
                    })
                })
                .collect();

            Self {
                runtime,
                state,
                workers,
            }
        }

        fn runtime(&self) -> *const FFI_VortexRuntime {
            self.runtime
        }

        /// Brings the workers down, so that scans and readers can be freed with nothing running.
        fn stop(&mut self) {
            self.state.stop.store(true, Ordering::Relaxed);
            self.state.condvar.notify_all();
            for worker in self.workers.drain(..) {
                worker.join().expect("worker panicked");
            }
        }
    }

    impl Drop for TestHost {
        fn drop(&mut self) {
            self.stop();
            unsafe { vortex_ffi_runtime_free(self.runtime) };
        }
    }

    /// Collects the chunks of a scan the way the ClickHouse reader does.
    struct TestConsumer {
        schema: Mutex<Option<SchemaRef>>,
        chunks: Mutex<Vec<(u64, RecordBatch)>>,
        finished: Mutex<Option<Option<String>>>,
        condvar: Condvar,
        /// Rejects this one split.
        fail_on_split: Option<u64>,
        /// Rejects every chunk, leaving no permit for anyone to give back.
        fail_all: bool,
        /// Delivered and not yet released, and the high-water mark of that.
        outstanding: AtomicUsize,
        max_outstanding: AtomicUsize,
    }

    impl TestConsumer {
        fn new(fail_on_split: Option<u64>) -> Arc<Self> {
            Self::with_failures(fail_on_split, false)
        }

        fn new_failing_all() -> Arc<Self> {
            Self::with_failures(None, true)
        }

        fn with_failures(fail_on_split: Option<u64>, fail_all: bool) -> Arc<Self> {
            Arc::new(Self {
                schema: Mutex::new(None),
                chunks: Mutex::new(Vec::new()),
                finished: Mutex::new(None),
                condvar: Condvar::new(),
                fail_on_split,
                fail_all,
                outstanding: AtomicUsize::new(0),
                max_outstanding: AtomicUsize::new(0),
            })
        }

        fn scan_callbacks(self: &Arc<Self>) -> FFI_VortexScanCallbacks {
            FFI_VortexScanCallbacks {
                context: Arc::as_ptr(self) as *mut c_void,
                on_chunk: test_on_chunk,
                on_finish: test_on_finish,
            }
        }

        fn wait(&self) -> Option<String> {
            self.wait_for(std::time::Duration::from_secs(60))
                .expect("the scan did not finish")
        }

        /// `None` if the end never arrived within `timeout`.
        fn wait_for(&self, timeout: std::time::Duration) -> Option<Option<String>> {
            let deadline = std::time::Instant::now() + timeout;
            let mut finished = self.finished.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                if let Some(error) = finished.as_ref() {
                    return Some(error.clone());
                }
                let now = std::time::Instant::now();
                if now >= deadline {
                    return None;
                }
                let (guard, _) = self
                    .condvar
                    .wait_timeout(finished, deadline - now)
                    .unwrap_or_else(|e| e.into_inner());
                finished = guard;
            }
        }

        fn rows(&self) -> usize {
            self.chunks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .map(|(_, batch)| batch.num_rows())
                .sum()
        }

        fn split_indices(&self) -> Vec<u64> {
            let mut indices: Vec<u64> = self
                .chunks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .map(|(index, _)| *index)
                .collect();
            indices.sort_unstable();
            indices
        }
    }

    unsafe extern "C" fn test_on_chunk(
        context: *mut c_void,
        array: *mut FFI_ArrowArray,
        split_index: u64,
    ) -> i32 {
        let consumer = unsafe { &*(context as *const TestConsumer) };
        // Rejecting a chunk does not hand it back: releasing it is still on us.
        if consumer.fail_all || consumer.fail_on_split == Some(split_index) {
            if !array.is_null() {
                drop(unsafe { std::ptr::read(array) });
            }
            return 1;
        }
        if array.is_null() {
            return 0;
        }
        let outstanding = consumer.outstanding.fetch_add(1, Ordering::Relaxed) + 1;
        consumer
            .max_outstanding
            .fetch_max(outstanding, Ordering::Relaxed);

        let schema = consumer
            .schema
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .expect("schema is set before the scan starts");
        let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref()).expect("schema");
        let data = from_ffi(unsafe { std::ptr::read(array) }, &ffi_schema).expect("from_ffi");
        let batch = RecordBatch::from(StructArray::from(data));
        consumer
            .chunks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((split_index, batch));
        0
    }

    unsafe extern "C" fn test_on_finish(context: *mut c_void, error: *const c_char) {
        let consumer = unsafe { &*(context as *const TestConsumer) };
        let message = if error.is_null() {
            None
        } else {
            Some(
                unsafe { CStr::from_ptr(error) }
                    .to_string_lossy()
                    .into_owned(),
            )
        };
        {
            let mut finished = consumer.finished.lock().unwrap_or_else(|e| e.into_inner());
            assert!(finished.is_none(), "on_finish called twice");
            *finished = Some(message);
        }
        consumer.condvar.notify_all();
    }

    fn test_batch(ids: Vec<i64>, names: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .expect("valid batch")
    }

    fn temporal_batch() -> RecordBatch {
        use arrow_array::{Date32Array, TimestampMicrosecondArray};
        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Date32, false),
            Field::new(
                "ts",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![10, 20, 30])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![1_000_000i64, 2_000_000, 3_000_000])
                        .with_timezone("UTC"),
                ),
            ],
        )
        .expect("valid batch")
    }

    fn scan_options() -> FFI_VortexScanOptions {
        FFI_VortexScanOptions {
            columns: std::ptr::null(),
            num_columns: 0,
            filter: std::ptr::null(),
            row_range_begin: 0,
            row_range_end: 0,
            max_splits_in_flight: 0,
        }
    }

    fn reader_options(
        io_concurrency: u32,
        coalesce: Option<(u64, u64)>,
    ) -> FFI_VortexReaderOptions {
        let (coalesce_max_gap_bytes, coalesce_max_read_bytes) = coalesce.unwrap_or((0, 0));
        FFI_VortexReaderOptions {
            io_concurrency,
            coalesce_max_gap_bytes,
            coalesce_max_read_bytes,
        }
    }

    unsafe fn open_reader(
        runtime: *const FFI_VortexRuntime,
        file: &mut TestFile,
        options: &FFI_VortexReaderOptions,
    ) -> *mut FFI_VortexReader {
        let mut error: *mut c_char = std::ptr::null_mut();
        let file_size = file.data.len() as u64;
        let reader = unsafe {
            vortex_ffi_reader_open(
                runtime,
                file.context(),
                read_from_vec,
                file_size,
                options,
                &mut error,
            )
        };
        assert!(!reader.is_null(), "{:?}", unsafe { CStr::from_ptr(error) });
        reader
    }

    unsafe fn write_file(batches: Vec<RecordBatch>) -> Vec<u8> {
        let mut file = Vec::<u8>::new();
        let mut error: *mut c_char = std::ptr::null_mut();
        unsafe {
            let mut ffi_schema =
                FFI_ArrowSchema::try_from(batches[0].schema().as_ref()).expect("schema");
            let writer = vortex_ffi_writer_create(
                &mut file as *mut Vec<u8> as *mut c_void,
                write_to_vec,
                &mut ffi_schema,
                &mut error,
            );
            std::mem::forget(ffi_schema);
            assert!(!writer.is_null(), "{:?}", CStr::from_ptr(error));

            for batch in batches {
                let (mut ffi_array, mut ffi_schema) =
                    to_ffi(&StructArray::from(batch).into_data()).expect("to_ffi");
                let result =
                    vortex_ffi_writer_write(writer, &mut ffi_array, &mut ffi_schema, &mut error);
                std::mem::forget(ffi_array);
                std::mem::forget(ffi_schema);
                assert_eq!(result, 0, "{:?}", CStr::from_ptr(error));
            }
            assert_eq!(vortex_ffi_writer_finish(writer, &mut error), 0);
            vortex_ffi_writer_free(writer);
        }
        file
    }

    /// The consumer has to be told the scan's schema before the scan exists, because it starts
    /// producing immediately.
    unsafe fn expected_scan_schema(
        reader: *mut FFI_VortexReader,
        options: &FFI_VortexScanOptions,
    ) -> SchemaRef {
        let mut error: *mut c_char = std::ptr::null_mut();
        let mut ffi_schema = FFI_ArrowSchema::empty();
        assert_eq!(
            unsafe { vortex_ffi_reader_schema(reader, &mut ffi_schema, &mut error) },
            0
        );
        let file_schema = Schema::try_from(&ffi_schema).expect("schema");
        if options.columns.is_null() {
            return Arc::new(file_schema);
        }
        let fields: Vec<Field> = (0..options.num_columns as usize)
            .map(|i| {
                let name = unsafe { CStr::from_ptr(*options.columns.add(i)) }
                    .to_str()
                    .expect("utf-8 name");
                file_schema
                    .field_with_name(name)
                    .expect("a column of the file")
                    .clone()
            })
            .collect();
        Arc::new(Schema::new(fields))
    }

    unsafe fn start_scan(
        reader: *mut FFI_VortexReader,
        options: &FFI_VortexScanOptions,
        consumer: &Arc<TestConsumer>,
    ) -> *mut FFI_VortexScan {
        let schema = unsafe { expected_scan_schema(reader, options) };
        *consumer.schema.lock().expect("lock") = Some(schema.clone());

        let scan_callbacks = consumer.scan_callbacks();
        let mut error: *mut c_char = std::ptr::null_mut();
        let scan = unsafe { vortex_ffi_scan_create(reader, options, &scan_callbacks, &mut error) };
        assert!(!scan.is_null(), "{:?}", unsafe { CStr::from_ptr(error) });

        let mut ffi_schema = FFI_ArrowSchema::empty();
        assert_eq!(
            unsafe { vortex_ffi_scan_schema(scan, &mut ffi_schema, &mut error) },
            0
        );
        assert_eq!(Schema::try_from(&ffi_schema).expect("schema"), *schema);
        scan
    }

    unsafe fn run_scan(
        reader: *mut FFI_VortexReader,
        options: &FFI_VortexScanOptions,
        fail_on_split: Option<u64>,
        release: bool,
    ) -> Arc<TestConsumer> {
        let consumer = TestConsumer::new(fail_on_split);
        let scan = unsafe { start_scan(reader, options, &consumer) };

        if release {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            loop {
                let released = consumer.outstanding.swap(0, Ordering::Relaxed);
                if released > 0 {
                    unsafe { vortex_ffi_scan_release(scan, released as u64) };
                }
                if consumer.finished.lock().expect("lock").is_some() {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the scan did not finish"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
        consumer.wait();
        unsafe { vortex_ffi_scan_free(scan) };
        consumer
    }

    /// Names of this process's threads. Linux only; empty anywhere else.
    fn thread_names() -> Vec<String> {
        let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
            return Vec::new();
        };
        tasks
            .flatten()
            .filter_map(|task| std::fs::read_to_string(task.path().join("comm")).ok())
            .map(|name| name.trim().to_string())
            .collect()
    }

    #[test]
    fn ffi_roundtrip() {
        let mut error: *mut c_char = std::ptr::null_mut();

        let file = unsafe {
            write_file(vec![
                test_batch(vec![1, 2, 3], vec![Some("a"), None, Some("c")]),
                test_batch(vec![4, 5], vec![Some("d"), Some("e")]),
            ])
        };
        assert_eq!(&file[0..4], b"VTXF");

        let host = TestHost::new(2);
        let mut test_file = TestFile::new(file.clone());
        unsafe {
            let reader = open_reader(host.runtime(), &mut test_file, &reader_options(1, None));
            assert_eq!(vortex_ffi_reader_row_count(reader), 5);

            let mut ffi_schema = FFI_ArrowSchema::empty();
            assert_eq!(
                vortex_ffi_reader_schema(reader, &mut ffi_schema, &mut error),
                0
            );
            let schema = Schema::try_from(&ffi_schema).expect("schema");
            assert_eq!(schema.field(0).name(), "id");
            assert_eq!(schema.field(1).name(), "name");

            let consumer = run_scan(reader, &scan_options(), None, true);
            assert_eq!(consumer.rows(), 5);
            let indices = consumer.split_indices();
            assert!(indices
                .iter()
                .enumerate()
                .all(|(i, index)| *index == i as u64));

            let column = CString::new("name").expect("valid name");
            let columns = [column.as_ptr()];
            let mut options = scan_options();
            options.columns = columns.as_ptr();
            options.num_columns = 1;
            let consumer = run_scan(reader, &options, None, true);
            assert_eq!(consumer.rows(), 5);
            let chunks = consumer.chunks.lock().expect("lock");
            assert_eq!(chunks[0].1.num_columns(), 1);
            assert_eq!(chunks[0].1.schema().field(0).name(), "name");
            drop(chunks);

            let id = CString::new("id").expect("valid name");
            let id_column = vortex_ffi_expr_column(id.as_ptr());
            let threshold = vortex_ffi_expr_literal_int(FFI_VortexPrimitiveType::I64, 2);
            let filter =
                vortex_ffi_expr_compare(FFI_VortexComparisonOperator::Gt, id_column, threshold);
            assert!(!filter.is_null());
            let mut options = scan_options();
            options.filter = filter;
            let consumer = run_scan(reader, &options, None, true);
            vortex_ffi_expr_free(filter);
            vortex_ffi_expr_free(threshold);
            vortex_ffi_expr_free(id_column);
            assert_eq!(consumer.rows(), 3);

            let mut options = scan_options();
            options.row_range_begin = 1;
            options.row_range_end = 4;
            let consumer = run_scan(reader, &options, None, true);
            assert_eq!(consumer.rows(), 3);

            let consumer = run_scan(reader, &scan_options(), Some(0), true);
            let error_message = consumer.wait().expect("the scan must fail");
            assert!(error_message.contains("convert"), "{error_message}");

            vortex_ffi_reader_free(reader);
        }

        unsafe {
            let mut truncated = TestFile::new(file[0..file.len() / 2].to_vec());
            let options = reader_options(1, None);
            let reader = vortex_ffi_reader_open(
                host.runtime(),
                truncated.context(),
                read_from_vec,
                truncated.data.len() as u64,
                &options,
                &mut error,
            );
            assert!(reader.is_null());
            assert!(!error.is_null());
            vortex_ffi_free_string(error);
        }
    }

    /// The temporal literals: a `vortex.date` / `vortex.timestamp` literal filters the matching
    /// rows, only the legal units build, and an expression renders for logging.
    #[test]
    fn ffi_temporal_literals() {
        let file = unsafe { write_file(vec![temporal_batch()]) };
        let host = TestHost::new(2);
        let mut test_file = TestFile::new(file);
        unsafe {
            let reader = open_reader(host.runtime(), &mut test_file, &reader_options(1, None));

            let date_column_name = CString::new("d").expect("valid name");
            let date_column = vortex_ffi_expr_column(date_column_name.as_ptr());
            let date_literal = vortex_ffi_expr_literal_date(FFI_VortexTimeUnit::Days, 15);
            assert!(!date_literal.is_null());
            let date_filter = vortex_ffi_expr_compare(
                FFI_VortexComparisonOperator::Gt,
                date_column,
                date_literal,
            );
            let mut options = scan_options();
            options.filter = date_filter;
            let consumer = run_scan(reader, &options, None, true);
            assert_eq!(consumer.rows(), 2);

            let rendered = vortex_ffi_expr_display(date_filter);
            assert!(!rendered.is_null());
            assert!(!CStr::from_ptr(rendered).to_bytes().is_empty());
            vortex_ffi_free_string(rendered);

            vortex_ffi_expr_free(date_filter);
            vortex_ffi_expr_free(date_literal);
            vortex_ffi_expr_free(date_column);

            let ts_column_name = CString::new("ts").expect("valid name");
            let ts_column = vortex_ffi_expr_column(ts_column_name.as_ptr());
            let timezone = CString::new("UTC").expect("valid name");
            let ts_literal = vortex_ffi_expr_literal_timestamp(
                FFI_VortexTimeUnit::Microseconds,
                timezone.as_ptr(),
                2_000_000,
            );
            assert!(!ts_literal.is_null());
            let ts_filter =
                vortex_ffi_expr_compare(FFI_VortexComparisonOperator::Lte, ts_column, ts_literal);
            let mut options = scan_options();
            options.filter = ts_filter;
            let consumer = run_scan(reader, &options, None, true);
            assert_eq!(consumer.rows(), 2);
            vortex_ffi_expr_free(ts_filter);
            vortex_ffi_expr_free(ts_literal);
            vortex_ffi_expr_free(ts_column);

            // Values and units that no literal exists for.
            assert!(vortex_ffi_expr_literal_date(
                FFI_VortexTimeUnit::Days,
                i64::from(i32::MAX) + 1
            )
            .is_null());
            assert!(vortex_ffi_expr_literal_date(FFI_VortexTimeUnit::Seconds, 1).is_null());
            assert!(vortex_ffi_expr_literal_timestamp(
                FFI_VortexTimeUnit::Days,
                std::ptr::null(),
                1
            )
            .is_null());

            vortex_ffi_reader_free(reader);
        }
    }

    /// Many splits over several worker threads: every row shows up once, and the scan never gets
    /// further ahead than it was allowed to.
    #[test]
    fn ffi_scan_on_host_threads() {
        let batches: Vec<RecordBatch> = (0..64)
            .map(|batch| {
                let ids: Vec<i64> = (0..10_000).map(|i| batch * 10_000 + i).collect();
                let names: Vec<Option<&str>> = ids.iter().map(|_| Some("x")).collect();
                test_batch(ids, names)
            })
            .collect();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let mut file = TestFile::new(unsafe { write_file(batches) });

        let host = TestHost::new(4);
        unsafe {
            let reader = open_reader(
                host.runtime(),
                &mut file,
                &reader_options(4, Some((1 << 20, 4 << 20))),
            );
            let mut options = scan_options();
            options.max_splits_in_flight = 3;
            let consumer = run_scan(reader, &options, None, true);
            assert_eq!(consumer.rows(), total_rows);

            let mut ids: Vec<i64> = consumer
                .chunks
                .lock()
                .expect("lock")
                .iter()
                .flat_map(|(_, batch)| {
                    batch
                        .column(0)
                        .as_primitive::<arrow_array::types::Int64Type>()
                        .values()
                        .iter()
                        .copied()
                        .collect::<Vec<i64>>()
                })
                .collect();
            ids.sort_unstable();
            assert_eq!(ids.len(), total_rows);
            assert!(ids.iter().enumerate().all(|(i, id)| *id == i as i64));

            let indices = consumer.split_indices();
            assert!(indices.len() > 1, "the file must have several splits");
            assert!(indices
                .iter()
                .enumerate()
                .all(|(i, index)| *index == i as u64));

            assert!(
                consumer.max_outstanding.load(Ordering::Relaxed)
                    <= options.max_splits_in_flight as usize,
                "{} chunks were outstanding, max_splits_in_flight is {}",
                consumer.max_outstanding.load(Ordering::Relaxed),
                options.max_splits_in_flight
            );

            vortex_ffi_reader_free(reader);
        }
    }

    /// A cancelled scan stops without announcing an end.
    #[test]
    fn ffi_scan_cancel() {
        let batches: Vec<RecordBatch> = (0..32)
            .map(|batch| {
                let ids: Vec<i64> = (0..10_000).map(|i| batch * 10_000 + i).collect();
                let names: Vec<Option<&str>> = ids.iter().map(|_| Some("x")).collect();
                test_batch(ids, names)
            })
            .collect();
        let mut file = TestFile::new(unsafe { write_file(batches) });

        let mut host = TestHost::new(2);
        unsafe {
            let reader = open_reader(
                host.runtime(),
                &mut file,
                &reader_options(2, Some((1 << 20, 4 << 20))),
            );
            let consumer = TestConsumer::new(None);
            let mut options = scan_options();
            options.max_splits_in_flight = 2;
            let scan = start_scan(reader, &options, &consumer);

            std::thread::sleep(std::time::Duration::from_millis(50));
            vortex_ffi_scan_cancel(scan);
            host.stop();
            assert!(
                consumer.finished.lock().expect("lock").is_none(),
                "a cancelled scan must not report its end"
            );
            vortex_ffi_scan_free(scan);
            vortex_ffi_reader_free(reader);
            drop(host);
        }
    }

    /// One I/O failure takes down every split in the air at once, so nothing is delivered and there
    /// is no chunk for the host to release. The driver is stuck waiting on a permit and still has
    /// to report.
    #[test]
    fn ffi_scan_reports_io_error_while_blocked_on_permits() {
        let mut state: u64 = 0x9E3779B97F4A7C15;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let batches: Vec<RecordBatch> = (0..32)
            .map(|_| {
                let ids: Vec<i64> = (0..10_000).map(|_| next() as i64).collect();
                let names: Vec<String> = (0..10_000).map(|_| format!("{:016x}", next())).collect();
                test_batch(ids, names.iter().map(|name| Some(name.as_str())).collect())
            })
            .collect();
        let data = unsafe { write_file(batches) };
        assert!(
            data.len() > 1 << 20,
            "test file is only {} bytes",
            data.len()
        );
        let mut file = TestFile::new(data);

        let mut host = TestHost::new(2);
        unsafe {
            let reader = open_reader(
                host.runtime(),
                &mut file,
                &reader_options(2, Some((1 << 20, 4 << 20))),
            );
            file.fail_reads.store(true, Ordering::Relaxed);
            let consumer = TestConsumer::new(None);
            let mut options = scan_options();
            options.max_splits_in_flight = 4;
            let scan = start_scan(reader, &options, &consumer);

            let outcome = consumer.wait_for(std::time::Duration::from_secs(10));
            vortex_ffi_scan_cancel(scan);
            host.stop();
            let error_message = outcome
                .expect("on_finish was never called after an I/O error")
                .expect("the scan must fail");
            assert!(
                error_message.contains("read callback failed"),
                "{error_message}"
            );
            vortex_ffi_scan_free(scan);
            vortex_ffi_reader_free(reader);
        }
    }

    /// The same standoff from the other side: the consumer rejects everything without cancelling,
    /// so no permit ever returns, and the first rejection still has to come out.
    #[test]
    fn ffi_scan_reports_error_when_every_chunk_is_rejected() {
        let batches: Vec<RecordBatch> = (0..64)
            .map(|batch| {
                let ids: Vec<i64> = (0..10_000).map(|i| batch * 10_000 + i).collect();
                let names: Vec<Option<&str>> = ids.iter().map(|_| Some("x")).collect();
                test_batch(ids, names)
            })
            .collect();
        let mut file = TestFile::new(unsafe { write_file(batches) });

        let mut host = TestHost::new(2);
        unsafe {
            let reader = open_reader(
                host.runtime(),
                &mut file,
                &reader_options(2, Some((1 << 20, 4 << 20))),
            );
            let consumer = TestConsumer::new_failing_all();
            let mut options = scan_options();
            options.max_splits_in_flight = 2;
            let scan = start_scan(reader, &options, &consumer);

            let outcome = consumer.wait_for(std::time::Duration::from_secs(10));
            vortex_ffi_scan_cancel(scan);
            host.stop();
            let error_message = outcome
                .expect("on_finish was never called while every chunk was rejected")
                .expect("the scan must fail");
            assert!(error_message.contains("convert"), "{error_message}");
            vortex_ffi_scan_free(scan);
            vortex_ffi_reader_free(reader);
        }
    }

    /// Merging turns neighbouring segments into a single call.
    #[test]
    fn ffi_read_coalescing() {
        let mut state: u64 = 0x9E3779B97F4A7C15;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let batches: Vec<RecordBatch> = (0..32)
            .map(|_| {
                let ids: Vec<i64> = (0..10_000).map(|_| next() as i64).collect();
                let names: Vec<String> = (0..10_000).map(|_| format!("{:016x}", next())).collect();
                test_batch(ids, names.iter().map(|name| Some(name.as_str())).collect())
            })
            .collect();
        let data = unsafe { write_file(batches) };
        assert!(
            data.len() > 1 << 20,
            "test file is only {} bytes",
            data.len()
        );

        let reads = |coalesce: Option<(u64, u64)>| -> usize {
            let host = TestHost::new(2);
            let mut file = TestFile::new(data.clone());
            unsafe {
                let reader = open_reader(host.runtime(), &mut file, &reader_options(1, coalesce));
                let consumer = run_scan(reader, &scan_options(), None, true);
                assert_eq!(consumer.rows(), 320_000);
                vortex_ffi_reader_free(reader);
            }
            file.reads()
        };

        let plain = reads(None);
        let coalesced = reads(Some((1 << 20, 4 << 20)));
        assert!(
            coalesced < plain,
            "coalesced {coalesced} reads, plain {plain} reads"
        );
    }

    /// Nothing in here owns a thread: no reactor and no blocking pool may be left standing.
    #[test]
    fn ffi_spawns_no_threads() {
        let batches: Vec<RecordBatch> = (0..8)
            .map(|batch| {
                let ids: Vec<i64> = (0..10_000).map(|i| batch * 10_000 + i).collect();
                let names: Vec<Option<&str>> = ids.iter().map(|_| Some("z")).collect();
                test_batch(ids, names)
            })
            .collect();
        let mut file = TestFile::new(unsafe { write_file(batches) });
        {
            let host = TestHost::new(2);
            unsafe {
                let reader = open_reader(
                    host.runtime(),
                    &mut file,
                    &reader_options(2, Some((1 << 20, 4 << 20))),
                );
                let consumer = run_scan(reader, &scan_options(), None, true);
                assert_eq!(consumer.rows(), 80_000);
                vortex_ffi_reader_free(reader);
            }
        }
        let names = thread_names();
        assert!(
            !names
                .iter()
                .any(|name| name.starts_with("async-io") || name.starts_with("blocking")),
            "unexpected library threads: {names:?}"
        );
    }

    /// Without a notification callback the runtime advances on the thread inside the call, so one
    /// thread on its own can carry a whole scan.
    #[test]
    fn ffi_scan_without_host_threads() {
        let batches: Vec<RecordBatch> = (0..8)
            .map(|batch| {
                let ids: Vec<i64> = (0..10_000).map(|i| batch * 10_000 + i).collect();
                let names: Vec<Option<&str>> = ids.iter().map(|_| Some("q")).collect();
                test_batch(ids, names)
            })
            .collect();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let mut file = TestFile::new(unsafe { write_file(batches) });

        unsafe {
            let runtime = vortex_ffi_runtime_new(std::ptr::null_mut(), None);
            let reader = open_reader(
                runtime,
                &mut file,
                &reader_options(1, Some((1 << 20, 4 << 20))),
            );
            let consumer = TestConsumer::new(None);
            let mut options = scan_options();
            options.max_splits_in_flight = 2;
            let scan = start_scan(reader, &options, &consumer);

            let mut error: *mut c_char = std::ptr::null_mut();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            while consumer.finished.lock().expect("lock").is_none() {
                let cpu = vortex_ffi_runtime_run(runtime, FFI_VortexTaskQueue::CPU, 4, &mut error);
                let io = vortex_ffi_runtime_run(runtime, FFI_VortexTaskQueue::IO, 4, &mut error);
                assert!(cpu >= 0 && io >= 0, "a task panicked");
                let released = consumer.outstanding.swap(0, Ordering::Relaxed);
                if released > 0 {
                    vortex_ffi_scan_release(scan, released as u64);
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the scan did not finish"
                );
            }
            assert_eq!(consumer.wait(), None);
            assert_eq!(consumer.rows(), total_rows);

            vortex_ffi_scan_free(scan);
            vortex_ffi_reader_free(reader);
            vortex_ffi_runtime_free(runtime);
        }
    }

    /// Sibling scans of one reader would share the read callback and each take the reader's whole
    /// `io_concurrency`, so the second one is refused, and freeing the first one gives the slot back.
    #[test]
    fn ffi_one_scan_per_reader() {
        let batches = vec![test_batch(vec![1, 2, 3], vec![Some("a"), None, Some("c")])];
        let mut file = TestFile::new(unsafe { write_file(batches) });
        let host = TestHost::new(1);
        unsafe {
            let reader = open_reader(host.runtime(), &mut file, &reader_options(1, None));
            let consumer = TestConsumer::new(None);
            let options = scan_options();
            let scan = start_scan(reader, &options, &consumer);

            let mut error: *mut c_char = std::ptr::null_mut();
            let scan_callbacks = consumer.scan_callbacks();
            let second = vortex_ffi_scan_create(reader, &options, &scan_callbacks, &mut error);
            assert!(second.is_null());
            let message = CStr::from_ptr(error).to_str().expect("utf-8").to_string();
            assert!(message.contains("already alive"), "{message}");
            vortex_ffi_free_string(error);

            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            loop {
                let released = consumer.outstanding.swap(0, Ordering::Relaxed);
                if released > 0 {
                    vortex_ffi_scan_release(scan, released as u64);
                }
                if consumer.finished.lock().expect("lock").is_some() {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the scan did not finish"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            consumer.wait();
            vortex_ffi_scan_free(scan);

            // The slot came back, so the reader takes another scan.
            let consumer = run_scan(reader, &options, None, true);
            assert_eq!(consumer.rows(), 3);
            vortex_ffi_reader_free(reader);
        }
    }
}
