//! C FFI bindings for the Vortex columnar file format (https://github.com/vortex-data/vortex),
//! used by ClickHouse's `Vortex` input/output formats.
//!
//! Data crosses the FFI boundary through the Arrow C Data Interface: the reader hands every scanned
//! chunk to a consumer callback as a `struct ArrowArray` (with one `struct ArrowSchema` per scan),
//! and the writer accepts record batches in the same representation. IO is delegated back to
//! ClickHouse through callbacks, so all reads and writes go through ClickHouse's own buffers (local
//! files, S3, HTTP, throttling, and so on).
//!
//! Threading model: the library owns no threads and no executor of its own. A `VortexFFIRuntime` is
//! a pair of task queues (CPU and IO) plus a notification callback: whenever a task becomes
//! runnable, the callback tells the host, and the host runs tasks by calling
//! `vortex_ffi_runtime_run` from as many of its own threads as it wants. This is the same shape as
//! any other work in ClickHouse - the host schedules short tasks on its thread pools - and it lets
//! CPU work and IO work go to different pools. A runtime with no notification callback is driven
//! only by the library itself, on the thread that is inside an FFI call (`vortex_ffi_reader_open`,
//! the writer functions), which is what the schema reader and the writer use.
//!
//! Scan results are pushed: the split task that produced a chunk calls the consumer's `on_chunk` on
//! whichever host thread ran it, so the conversion of the chunk happens in parallel too, and the
//! scan reports its end (or its first error) through `on_finish`. The number of chunks that may be
//! outstanding is bounded by `in_flight`; the host returns capacity with `vortex_ffi_scan_release`.
//!
//! Error convention: fallible functions take a `char ** error` out-parameter. On failure it is
//! set to a heap-allocated message that must be freed with `vortex_ffi_free_string`.

use std::any::Any;
use std::ffi::{c_char, c_void, CStr, CString};
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

/// Reads `length` bytes at `offset` into `out`. Returns 0 on success, non-zero on failure.
pub type VortexFFIReadCallback =
    unsafe extern "C" fn(context: *mut c_void, offset: u64, length: u64, out: *mut u8) -> i32;

/// Consumes `length` bytes from `data`. Returns 0 on success, non-zero on failure.
pub type VortexFFIWriteCallback =
    unsafe extern "C" fn(context: *mut c_void, data: *const u8, length: u64) -> i32;

/// The queue a task belongs to.
#[repr(i32)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum VortexFFIQueue {
    /// Decoding, filtering, Arrow export: tasks that use the CPU.
    Cpu = 0,
    /// Tasks that call the read callback.
    Io = 1,
}

const NUM_QUEUES: usize = 2;

/// Called when a task becomes runnable on the given queue of the runtime. It must not call back
/// into the library; the host is expected to schedule `vortex_ffi_runtime_run` somewhere and return.
/// May be called from any thread, including from inside `vortex_ffi_runtime_run` itself.
pub type VortexFFINotifyCallback = unsafe extern "C" fn(context: *mut c_void, queue: VortexFFIQueue);

/// The task queues of one reader (or writer) and the way to tell the host that a task is runnable.
///
/// This is the whole of the library's threading: futures spawned by Vortex become `Runnable`s in
/// one of the two queues, and they only ever run inside `vortex_ffi_runtime_run` (or inside
/// `block_on`, on the thread of an FFI call).
struct HostRuntime {
    queues: [ConcurrentQueue<Runnable>; NUM_QUEUES],
    notify: Option<VortexFFINotifyCallback>,
    /// The opaque host pointer passed to `notify`, as an integer so that the struct stays `Send`.
    context: usize,
    /// A weak reference to itself, used by the schedule functions: a task that outlives the runtime
    /// must be dropped instead of queued.
    weak_self: Weak<HostRuntime>,
    /// The threads that are inside `block_on` on this runtime and have to be woken up when a task
    /// is queued (they may be parked waiting for it).
    parked: Mutex<Vec<(u64, parking::Unparker)>>,
    num_parked: AtomicUsize,
    next_parked_id: AtomicU64,
}

impl HostRuntime {
    fn new(context: usize, notify: Option<VortexFFINotifyCallback>) -> Arc<Self> {
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

    /// A handle for Vortex to spawn tasks with. It is a weak reference: the runtime is kept alive by
    /// the reader or writer that owns it.
    fn handle(self: &Arc<Self>) -> Handle {
        let executor: Arc<dyn Executor> = self.clone();
        Handle::new(Arc::downgrade(&executor))
    }

    fn spawn_on(&self, queue: VortexFFIQueue, future: BoxFuture<'static, ()>) -> AbortHandleRef {
        let weak = self.weak_self.clone();
        let schedule = move |runnable: Runnable| match weak.upgrade() {
            // Dropping the runnable drops the future: the task is cancelled, which is what should
            // happen to work scheduled after its runtime is gone.
            None => drop(runnable),
            Some(runtime) => runtime.enqueue(queue, runnable),
        };
        // `Runnable::run` re-raises a panic of the future on the thread that runs it; catch it here
        // so that a panic in a Vortex task never unwinds into the host's thread pool. The task's
        // own error reporting (the scan consumer) is done by the future itself.
        let (runnable, task) = async_task::spawn(
            async move {
                let _ = AssertUnwindSafe(future).catch_unwind().await;
            },
            schedule,
        );
        runnable.schedule();
        Box::new(HostAbortHandle { task: Some(task) })
    }

    fn enqueue(&self, queue: VortexFFIQueue, runnable: Runnable) {
        // The queues are unbounded and never closed, so pushing cannot fail.
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

    /// Runs at most `max_tasks` tasks of the queue, returning how many were run.
    fn run(&self, queue: VortexFFIQueue, max_tasks: usize) -> usize {
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

    fn pending(&self, queue: VortexFFIQueue) -> usize {
        self.queues[queue as usize].len()
    }

    /// Runs the runtime on the calling thread until `future` completes.
    ///
    /// Used for the operations that are synchronous from the host's point of view (opening a file,
    /// writing a batch): the calling thread runs the tasks they spawn, so they work with or without
    /// a host driving the runtime.
    fn block_on<F: Future>(self: &Arc<Self>, future: F) -> F::Output {
        let parker = parking::Parker::new();
        let unparker = parker.unparker();
        let id = self.next_parked_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut parked = self.parked.lock().unwrap_or_else(|e| e.into_inner());
            parked.push((id, unparker.clone()));
        }
        // Published after the unparker is in the list, so that a concurrent `enqueue` that observes
        // the count also observes the unparker.
        self.num_parked.fetch_add(1, Ordering::Release);

        let waker = Waker::from(unparker);
        let mut context = Context::from_waker(&waker);
        let mut future = pin!(future);
        let output = loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                break output;
            }
            // A host thread may be running the same queues in parallel; whoever gets the task runs
            // it, and the waker wakes this thread up when the future can make progress.
            if self.run(VortexFFIQueue::Cpu, 1) > 0 || self.run(VortexFFIQueue::Io, 1) > 0 {
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
        self.spawn_on(VortexFFIQueue::Cpu, future)
    }

    fn spawn_io(&self, future: BoxFuture<'static, ()>) -> AbortHandleRef {
        self.spawn_on(VortexFFIQueue::Io, future)
    }

    fn spawn_cpu(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
        self.spawn_on(VortexFFIQueue::Cpu, async move { task() }.boxed())
    }

    fn spawn_blocking_io(&self, task: Box<dyn FnOnce() + Send + 'static>) -> AbortHandleRef {
        self.spawn_on(VortexFFIQueue::Io, async move { task() }.boxed())
    }
}

/// Aborting cancels the task: dropping an `async_task::Task` drops the future as soon as the task is
/// idle. Dropping the handle without aborting detaches the task, which then runs to completion.
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

/// The FFI handle of a `HostRuntime`.
pub struct VortexFFIRuntime {
    inner: Arc<HostRuntime>,
}

/// Creates a runtime. `notify` (which may be null, together with `context`) is called whenever a
/// task becomes runnable; the host is then expected to call `vortex_ffi_runtime_run` for that queue
/// from one of its threads. A runtime without a callback only ever runs tasks on the threads that
/// are inside FFI calls on it.
///
/// The runtime must outlive the readers, scans and writers created on it.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_runtime_new(
    context: *mut c_void,
    notify: Option<VortexFFINotifyCallback>,
) -> *mut VortexFFIRuntime {
    Box::into_raw(Box::new(VortexFFIRuntime { inner: HostRuntime::new(context as usize, notify) }))
}

/// Runs at most `max_tasks` runnable tasks of the given queue (0 means no limit) and returns how
/// many were run, or -1 if a task panicked (the panic does not cross the FFI boundary).
///
/// Thread-safe: any number of threads may run the same queue at once. A task may queue more tasks,
/// including on the other queue, which is reported through the notification callback.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_runtime_run(
    runtime: *const VortexFFIRuntime,
    queue: VortexFFIQueue,
    max_tasks: u32,
    error: *mut *mut c_char,
) -> i64 {
    unsafe {
        ffi_wrap(error, -1, || {
            let runtime = &*runtime;
            let max_tasks = if max_tasks == 0 { usize::MAX } else { max_tasks as usize };
            Ok(runtime.inner.run(queue, max_tasks) as i64)
        })
    }
}

/// Returns the number of tasks waiting in the given queue. Thread-safe.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_runtime_pending(
    runtime: *const VortexFFIRuntime,
    queue: VortexFFIQueue,
) -> u64 {
    unsafe { (*runtime).inner.pending(queue) as u64 }
}

/// Frees the runtime. Everything created on it must be freed first, and no thread may be inside
/// `vortex_ffi_runtime_run` on it.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_runtime_free(runtime: *mut VortexFFIRuntime) {
    if !runtime.is_null() {
        unsafe { drop(Box::from_raw(runtime)) };
    }
}

pub struct VortexFFIReader {
    session: VortexSession,
    runtime: Arc<HostRuntime>,
    file: VortexFile,
    schema: SchemaRef,
}

pub struct VortexFFIScan {
    /// The canonical Arrow schema of the projected columns.
    schema: SchemaRef,
    /// The capacity of the scan: a permit is taken before a split task is spawned and returned when
    /// the host releases the chunk it produced. Closing it cancels the scan.
    permits: kanal::AsyncReceiver<()>,
    /// The task that spawns the split tasks and reports the end of the scan. Dropping it cancels
    /// the scan together with the split tasks it owns.
    driver: Mutex<Option<Task<()>>>,
}

/// A node of a filter expression built through `vortex_ffi_expr_*`.
pub struct VortexFFIExpression(Expression);

pub struct VortexFFIWriter {
    session: VortexSession,
    runtime: Arc<HostRuntime>,
    schema: SchemaRef,
    writer: Option<vortex::file::Writer<'static>>,
}
unsafe fn set_error(error: *mut *mut c_char, message: String)
{
    if error.is_null() {
        return;
    }
    let message = CString::new(message.replace('\0', " "))
        .unwrap_or_else(|_| CString::new("invalid error message").expect("valid literal"));
    unsafe {
        *error = message.into_raw();
    }
}

/// The message of a caught panic.
fn panic_message(panic: &(dyn Any + Send)) -> String {
    let message = panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_string());
    format!("panic: {message}")
}

/// Runs `f`, catching both errors and panics. On failure stores the message into `error` and
/// returns `on_error`.
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

/// A `VortexReadAt` implementation on top of a ClickHouse read callback.
///
/// A read is a task on the IO queue, so it blocks whichever host thread runs that queue (the
/// callback is synchronous) while the CPU queue keeps decoding. Up to `concurrency` reads may be in
/// flight at once.
#[derive(Clone)]
struct CallbackReader {
    context: usize,
    read: VortexFFIReadCallback,
    size: u64,
    concurrency: usize,
    /// How the file's segment source merges nearby segment reads into one callback invocation.
    /// `None` means one callback per segment.
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
                if offset.checked_add(length as u64).is_none_or(|end| end > this.size) {
                    return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
                }
                // The callback either fills all `length` bytes or fails, so the buffer does not
                // need to be zero-initialized.
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

/// Options of `vortex_ffi_reader_open`. A zero-initialized struct means: one read at a time and
/// no coalescing.
#[repr(C)]
pub struct VortexFFIReaderOptions {
    /// The maximum number of reads the library may have in flight at once (0 or 1 = one). The
    /// read callback must be thread-safe if it is greater than 1.
    pub io_concurrency: u32,
    /// Nearby segment reads are merged into one callback invocation when the gap between them is
    /// at most `coalesce_distance` bytes and the merged read is at most `coalesce_max_size` bytes.
    /// Both zero disables coalescing: one callback per segment.
    pub coalesce_distance: u64,
    pub coalesce_max_size: u64,
}

/// Opens a Vortex file for reading on the given runtime. The file is accessed through `read` with
/// the given opaque `context`; `file_size` must be the exact file size in bytes; `options` may be
/// null (the defaults).
///
/// Reading the footer needs IO, which happens on the calling thread (the tasks it spawns are run by
/// it), so this function does not require the host to be driving the runtime yet.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_reader_open(
    runtime: *const VortexFFIRuntime,
    context: *mut c_void,
    read: VortexFFIReadCallback,
    file_size: u64,
    options: *const VortexFFIReaderOptions,
    error: *mut *mut c_char,
) -> *mut VortexFFIReader {
    unsafe {
        ffi_wrap(error, std::ptr::null_mut(), || {
            let runtime = (*runtime).inner.clone();
            let mut concurrency = 1usize;
            let mut coalesce = None;
            if !options.is_null() {
                let options = &*options;
                concurrency = std::cmp::max(options.io_concurrency, 1) as usize;
                if options.coalesce_distance != 0 || options.coalesce_max_size != 0 {
                    coalesce =
                        Some(CoalesceConfig::new(options.coalesce_distance, options.coalesce_max_size));
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
                .block_on(session.open_options().with_file_size(file_size).open_read(source))
                .map_err(|e| e.to_string())?;
            let schema = Arc::new(
                session
                    .arrow()
                    .to_arrow_schema(file.dtype())
                    .map_err(|e| e.to_string())?,
            );
            Ok(Box::into_raw(Box::new(VortexFFIReader { session, runtime, file, schema })))
        })
    }
}

/// Returns the total number of rows in the file.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_reader_row_count(reader: *const VortexFFIReader) -> u64 {
    unsafe { (*reader).file.row_count() }
}

/// Exports the file schema into `out_schema` (an Arrow C Data Interface `struct ArrowSchema`).
/// The caller takes ownership and must release it. Returns 0 on success.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_reader_schema(
    reader: *const VortexFFIReader,
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

#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_reader_free(reader: *mut VortexFFIReader) {
    if !reader.is_null() {
        unsafe { drop(Box::from_raw(reader)) };
    }
}
struct CallbackWrite {
    context: usize,
    write: VortexFFIWriteCallback,
}

impl VortexWrite for CallbackWrite {
    fn write_all<B: IoBuf>(&mut self, buffer: B) -> impl std::future::Future<Output = std::io::Result<B>> {
        let slice = buffer.as_slice();
        let result = unsafe { (self.write)(self.context as *mut c_void, slice.as_ptr(), slice.len() as u64) };
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


/// Options of a scan, see `vortex_ffi_scan_create`. All fields are optional: a zero-initialized
/// struct scans all rows of all columns.
#[repr(C)]
pub struct VortexFFIScanOptions {
    /// The names of the top-level columns to read, in the given order. Null means all columns.
    pub columns: *const *const c_char,
    pub num_columns: u64,
    /// The filter expression; only the rows matching it are returned. Null means no filter.
    pub filter: *const VortexFFIExpression,
    /// The row range `[row_range_begin, row_range_end)` to scan. Both zero means the whole file.
    pub row_range_begin: u64,
    pub row_range_end: u64,
    /// The maximum number of chunks that may be in flight at once: being read, decoded, or already
    /// handed to `on_chunk` and not yet released with `vortex_ffi_scan_release` (0 = default).
    /// This bounds both the memory the scan holds and the amount of IO lookahead.
    pub in_flight: u32,
}

/// The callbacks a scan reports to. They are called on the host threads that run the scan's tasks,
/// concurrently, and must not call back into the library (except `vortex_ffi_scan_release`, which
/// must not be called from `on_chunk` itself).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VortexFFIScanConsumer {
    pub context: *mut c_void,
    /// Receives one chunk of the scan: an Arrow struct array in the scan schema, whose ownership
    /// passes to the consumer, and the 0-based index of its row split in file order. A null array
    /// means the split matched no rows (reported so that the consumer can restore the file order).
    /// Returns 0 on success; a non-zero return stops the scan and is reported as an error to
    /// `on_finish`.
    pub on_chunk: unsafe extern "C" fn(
        context: *mut c_void,
        array: *mut FFI_ArrowArray,
        split_index: u64,
    ) -> i32,
    /// Called exactly once when the scan ends: with null when all splits were delivered, or with an
    /// error message (valid only during the call) when the scan failed. Not called when the scan is
    /// cancelled with `vortex_ffi_scan_cancel`.
    pub on_finish: unsafe extern "C" fn(context: *mut c_void, error: *const c_char),
}

/// The consumer as it is captured by the tasks: the raw pointer is kept as an integer so that the
/// struct is `Send`. The host guarantees the context outlives the scan.
#[derive(Clone, Copy)]
struct Consumer {
    context: usize,
    on_chunk: unsafe extern "C" fn(*mut c_void, *mut FFI_ArrowArray, u64) -> i32,
    on_finish: unsafe extern "C" fn(*mut c_void, *const c_char),
}

impl Consumer {
    fn deliver(&self, array: Option<FFI_ArrowArray>, split_index: u64) -> VortexResult<()> {
        let empty = array.is_none();
        let result = match array {
            None => unsafe {
                (self.on_chunk)(self.context as *mut c_void, std::ptr::null_mut(), split_index)
            },
            Some(array) => {
                // The ownership of the array passes to the consumer, which either imports it or
                // releases it, so it must not be dropped (released) here as well.
                let mut array = std::mem::ManuallyDrop::new(array);
                unsafe {
                    (self.on_chunk)(self.context as *mut c_void, &mut *array, split_index)
                }
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
                let message = CString::new(message.replace('\0', " "))
                    .unwrap_or_else(|_| CString::new("invalid error message").expect("valid literal"));
                unsafe { (self.on_finish)(self.context as *mut c_void, message.as_ptr()) };
            }
        }
    }
}

/// Reports the end of a scan to the consumer exactly once, also when the driver task panics: the
/// host waits for `on_finish` and would otherwise wait forever. Nothing is reported when the driver
/// is cancelled (dropped without a panic), as the contract of `vortex_ffi_scan_cancel` requires.
struct FinishGuard {
    consumer: Consumer,
    finished: bool,
}

impl FinishGuard {
    fn finish(mut self, error: Option<String>) {
        self.finished = true;
        self.consumer.finish(error);
    }
}

impl Drop for FinishGuard {
    fn drop(&mut self) {
        // The locals of the driver are dropped while its panic unwinds out of `poll`, before the
        // runtime catches the panic, so this tells a panic from a cancellation.
        if !self.finished && std::thread::panicking() {
            self.consumer.finish(Some("panic in the scan driver".to_string()));
        }
    }
}

const DEFAULT_IN_FLIGHT: usize = 4;

/// The error of one joined split task, if it failed or panicked.
fn scan_task_error(outcome: Result<VortexResult<()>, Box<dyn Any + Send>>) -> Option<String> {
    match outcome {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(e.to_string()),
        Err(panic) => Some(panic_message(panic.as_ref())),
    }
}

/// Creates a scan over the file and starts it: the split tasks are spawned onto the reader's
/// runtime as capacity allows, and every chunk they produce is handed to `consumer.on_chunk` on the
/// thread that ran the task. The end of the scan (or its first error) is reported to
/// `consumer.on_finish`.
///
/// Expression optimization and split computation happen here, on the calling thread. The reader and
/// the consumer's context must stay alive for the whole lifetime of the scan; `filter` is not
/// consumed. Returns null on error.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_scan_create(
    reader: *const VortexFFIReader,
    options: *const VortexFFIScanOptions,
    consumer: *const VortexFFIScanConsumer,
    error: *mut *mut c_char,
) -> *mut VortexFFIScan {
    unsafe {
        ffi_wrap(error, std::ptr::null_mut(), || {
            let reader = &*reader;
            let mut builder = reader.file.scan().map_err(|e| e.to_string())?;

            let mut schema = reader.schema.clone();
            let mut in_flight = DEFAULT_IN_FLIGHT;

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
                            reader.schema.field_with_name(name).map_err(|e| e.to_string())?.clone(),
                        );
                    }
                    let field_names: Vec<FieldName> =
                        names.iter().map(|name| FieldName::from(name.as_str())).collect();
                    builder = builder.with_projection(select(field_names, root()));
                    schema = Arc::new(Schema::new(fields));
                }

                if !options.filter.is_null() {
                    builder = builder.with_filter((*options.filter).0.clone());
                }

                if options.row_range_begin != 0 || options.row_range_end != 0 {
                    if options.row_range_begin > options.row_range_end {
                        return Err(format!(
                            "invalid row range [{}, {})",
                            options.row_range_begin, options.row_range_end
                        ));
                    }
                    builder = builder.with_row_range(options.row_range_begin..options.row_range_end);
                }

                if options.in_flight != 0 {
                    in_flight = options.in_flight as usize;
                }
            }

            let consumer = {
                let consumer = &*consumer;
                Consumer {
                    context: consumer.context as usize,
                    on_chunk: consumer.on_chunk,
                    on_finish: consumer.on_finish,
                }
            };

            // Every chunk is exported to Arrow inside its split task, i.e. on whichever host thread
            // runs the task, and checked against the scan schema so that the consumer can import all
            // chunks with the single schema returned by `vortex_ffi_scan_schema`.
            let session = reader.session.clone();
            let struct_field = Field::new_struct("", schema.fields().clone(), false);
            let expected_type = struct_field.data_type().clone();
            let builder = builder.map(move |chunk| {
                let mut ctx = session.create_execution_ctx();
                let arrow = session.arrow().execute_arrow(chunk, Some(&struct_field), &mut ctx)?;
                if arrow.data_type() != &expected_type {
                    return Err(vortex_err!(
                        "Vortex chunk exported as {} instead of the scan schema {}",
                        arrow.data_type(),
                        expected_type
                    ));
                }
                Ok(FFI_ArrowArray::new(&arrow.as_struct().to_data()))
            });

            // Note: `ScanBuilder::into_stream` / `into_iter` are deliberately not used here. They
            // wrap the scan in the library's `LazyScanStream`, which offloads `ScanBuilder::prepare`
            // to the `blocking` crate's global thread pool and joins it through a `oneshot`
            // channel. That contradicts our threading model (see the module documentation): work
            // would run on threads ClickHouse knows nothing about. `prepare` and `execute` are
            // pure, synchronous computation (expression optimization and split computation, no
            // IO), so we run them here and spawn the resulting per-split tasks ourselves.
            let tasks = builder
                .prepare()
                .and_then(|scan| scan.execute(None))
                .map_err(|e| e.to_string())?;

            let in_flight = std::cmp::max(in_flight, 1);
            // One permit per chunk that may be outstanding. A permit is taken before its split task
            // is spawned and returned when the host releases the chunk (or right away for a split
            // that produced no rows), so the scan never runs further ahead than the host consumes.
            // Closing the channel cancels the scan.
            let (permit_sender, permits) = kanal::bounded_async::<()>(in_flight);

            let handle = reader.runtime.handle();
            let spawner = handle.clone();
            let permits_for_tasks = permits.clone();

            // The driver: it spawns one task per split as capacity allows, and joins the spawned
            // tasks to observe their errors. The tasks themselves run independently on the host's
            // threads, so the driver blocking on a permit does not hold the scan back.
            let driver = handle.spawn(async move {
                let mut spawned = futures::stream::FuturesUnordered::new();
                let mut error: Option<String> = None;
                let guard = FinishGuard { consumer, finished: false };

                for (split_index, task) in tasks.into_iter().enumerate() {
                    // Waits until the host releases a chunk, joining the split tasks that finish
                    // meanwhile: an error must stop the scan even while the host holds every
                    // permit, since a failed split delivers nothing that the host could release.
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
                                // Fails only when the scan is cancelled, and then there is nobody
                                // to report to.
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
                            Ok(Some(array)) => consumer.deliver(Some(array), split_index),
                            Ok(None) => {
                                // No rows: nothing is delivered to the host, so the permit of this
                                // split has to be returned here.
                                let result = consumer.deliver(None, split_index);
                                let _ = permits.try_recv();
                                result
                            }
                            Err(e) => Err(e),
                        }
                    });
                    // A panic in the split task is re-raised when the task is joined; catch it here
                    // so that it reaches the consumer as an error.
                    spawned.push(AssertUnwindSafe(task).catch_unwind());
                }

                while error.is_none() {
                    match spawned.next().await {
                        None => break,
                        Some(outcome) => error = scan_task_error(outcome),
                    }
                }

                // Cancels the split tasks that are still in flight after an error, before the
                // consumer is told that the scan is over.
                drop(spawned);
                guard.finish(error);
            });

            Ok(Box::into_raw(Box::new(VortexFFIScan {
                schema,
                permits,
                driver: Mutex::new(Some(driver)),
            })))
        })
    }
}

/// Exports the schema of the chunks produced by the scan into `out_schema` (an Arrow C Data
/// Interface `struct ArrowSchema`). The caller takes ownership and must release it. Returns 0 on
/// success.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_scan_schema(
    scan: *const VortexFFIScan,
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

/// Returns the capacity of `count` chunks that were delivered to `on_chunk` and are not needed by
/// the host anymore, letting the scan read that many splits further ahead.
///
/// Thread-safe, and safe to call after the scan has finished or was cancelled (it does nothing
/// then). Must not be called from inside `on_chunk`.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_scan_release(scan: *const VortexFFIScan, count: u64) {
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

/// Cancels the scan. Thread-safe. The pending split tasks are dropped; `on_chunk` and `on_finish`
/// are not called anymore once this returns, except from a task that is running at that moment, so
/// the host must stop running the runtime's queues before it frees the consumer's context.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_scan_cancel(scan: *const VortexFFIScan) {
    if scan.is_null() {
        return;
    }
    let scan = unsafe { &*scan };
    // Stops the spawner: no more split tasks are created.
    let _ = scan.permits.close();
    // Dropping the driver task cancels it, together with the split tasks it owns.
    let driver = scan.driver.lock().unwrap_or_else(|e| e.into_inner()).take();
    drop(driver);
}

/// Frees the scan. The host must have stopped running the runtime's queues (no task of this scan
/// may be running).
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_scan_free(scan: *mut VortexFFIScan) {
    if !scan.is_null() {
        unsafe { drop(Box::from_raw(scan)) };
    }
}
/// The primitive type of a literal built through `vortex_ffi_expr_literal_*`. Must match the
/// exact type of the file column it is compared with: Vortex comparisons require both sides to
/// have the same type.
#[repr(i32)]
#[derive(Clone, Copy)]
pub enum VortexFFIPType {
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

/// A comparison operator for `vortex_ffi_expr_compare`.
#[repr(i32)]
#[derive(Clone, Copy)]
pub enum VortexFFIComparison {
    Eq = 0,
    NotEq = 1,
    Lt = 2,
    Lte = 3,
    Gt = 4,
    Gte = 5,
}

/// The expression builders below return null on invalid input. They do not consume their
/// arguments: every returned handle must be freed with `vortex_ffi_expr_free`.

/// Creates an expression referencing the top-level column `name`.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_column(name: *const c_char) -> *mut VortexFFIExpression {
    if name.is_null() {
        return std::ptr::null_mut();
    }
    let Ok(name) = (unsafe { CStr::from_ptr(name) }).to_str() else {
        return std::ptr::null_mut();
    };
    let expr = get_item(FieldName::from(name), root());
    Box::into_raw(Box::new(VortexFFIExpression(expr)))
}

/// Creates a signed integer literal of the given type. Returns null if the value does not fit.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_int(
    ptype: VortexFFIPType,
    value: i64,
) -> *mut VortexFFIExpression {
    let nullability = Nullability::NonNullable;
    let scalar = match ptype {
        VortexFFIPType::I8 => match i8::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        VortexFFIPType::I16 => match i16::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        VortexFFIPType::I32 => match i32::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        VortexFFIPType::I64 => Scalar::primitive(value, nullability),
        _ => return std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(VortexFFIExpression(lit(scalar))))
}

/// Creates an unsigned integer literal of the given type. Returns null if the value does not fit.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_uint(
    ptype: VortexFFIPType,
    value: u64,
) -> *mut VortexFFIExpression {
    let nullability = Nullability::NonNullable;
    let scalar = match ptype {
        VortexFFIPType::U8 => match u8::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        VortexFFIPType::U16 => match u16::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        VortexFFIPType::U32 => match u32::try_from(value) {
            Ok(v) => Scalar::primitive(v, nullability),
            Err(_) => return std::ptr::null_mut(),
        },
        VortexFFIPType::U64 => Scalar::primitive(value, nullability),
        _ => return std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(VortexFFIExpression(lit(scalar))))
}

/// Creates a floating-point literal of the given type. For `F32` the value must be exactly
/// representable as `f32`, otherwise null is returned (a rounded bound would change the
/// comparison result).
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_float(
    ptype: VortexFFIPType,
    value: f64,
) -> *mut VortexFFIExpression {
    let nullability = Nullability::NonNullable;
    let scalar = match ptype {
        VortexFFIPType::F32 => {
            let narrowed = value as f32;
            if f64::from(narrowed) != value {
                return std::ptr::null_mut();
            }
            Scalar::primitive(narrowed, nullability)
        }
        VortexFFIPType::F64 => Scalar::primitive(value, nullability),
        _ => return std::ptr::null_mut(),
    };
    Box::into_raw(Box::new(VortexFFIExpression(lit(scalar))))
}

/// Creates a boolean literal.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_bool(value: bool) -> *mut VortexFFIExpression {
    let scalar = Scalar::bool(value, Nullability::NonNullable);
    Box::into_raw(Box::new(VortexFFIExpression(lit(scalar))))
}

/// Creates a string literal: `Utf8` if `is_utf8` is true (the bytes must be valid UTF-8,
/// otherwise null is returned), `Binary` otherwise. A null `data` is accepted only for an empty
/// literal (`length` 0), which is the empty string.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_literal_string(
    data: *const u8,
    length: u64,
    is_utf8: bool,
) -> *mut VortexFFIExpression {
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
    Box::into_raw(Box::new(VortexFFIExpression(lit(scalar))))
}

/// Creates a comparison `lhs op rhs`. A comparison with a null value yields null, which the
/// scan treats as "row does not match".
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_compare(
    comparison: VortexFFIComparison,
    lhs: *const VortexFFIExpression,
    rhs: *const VortexFFIExpression,
) -> *mut VortexFFIExpression {
    if lhs.is_null() || rhs.is_null() {
        return std::ptr::null_mut();
    }
    let operator = match comparison {
        VortexFFIComparison::Eq => Operator::Eq,
        VortexFFIComparison::NotEq => Operator::NotEq,
        VortexFFIComparison::Lt => Operator::Lt,
        VortexFFIComparison::Lte => Operator::Lte,
        VortexFFIComparison::Gt => Operator::Gt,
        VortexFFIComparison::Gte => Operator::Gte,
    };
    let expr =
        unsafe { Binary.new_expr(operator, [(*lhs).0.clone(), (*rhs).0.clone()]) };
    Box::into_raw(Box::new(VortexFFIExpression(expr)))
}

/// Creates a Kleene (three-valued) AND of two boolean expressions.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_and(
    lhs: *const VortexFFIExpression,
    rhs: *const VortexFFIExpression,
) -> *mut VortexFFIExpression {
    if lhs.is_null() || rhs.is_null() {
        return std::ptr::null_mut();
    }
    let expr =
        unsafe { Binary.new_expr(Operator::And, [(*lhs).0.clone(), (*rhs).0.clone()]) };
    Box::into_raw(Box::new(VortexFFIExpression(expr)))
}

/// Creates a Kleene (three-valued) OR of two boolean expressions.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_or(
    lhs: *const VortexFFIExpression,
    rhs: *const VortexFFIExpression,
) -> *mut VortexFFIExpression {
    if lhs.is_null() || rhs.is_null() {
        return std::ptr::null_mut();
    }
    let expr =
        unsafe { Binary.new_expr(Operator::Or, [(*lhs).0.clone(), (*rhs).0.clone()]) };
    Box::into_raw(Box::new(VortexFFIExpression(expr)))
}

/// Creates a logical NOT of a boolean expression (NOT of null is null).
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_not(
    child: *const VortexFFIExpression,
) -> *mut VortexFFIExpression {
    if child.is_null() {
        return std::ptr::null_mut();
    }
    let expr = unsafe { not((*child).0.clone()) };
    Box::into_raw(Box::new(VortexFFIExpression(expr)))
}

/// Creates an expression that is true where the child expression is null.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_is_null(
    child: *const VortexFFIExpression,
) -> *mut VortexFFIExpression {
    if child.is_null() {
        return std::ptr::null_mut();
    }
    let expr = unsafe { is_null((*child).0.clone()) };
    Box::into_raw(Box::new(VortexFFIExpression(expr)))
}

#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_expr_free(expr: *mut VortexFFIExpression) {
    if !expr.is_null() {
        unsafe { drop(Box::from_raw(expr)) };
    }
}


/// Creates a writer producing a Vortex file with the given schema (consumed). The bytes of the
/// file are sent to `write` with the given opaque `context`.
///
/// The writer drives its own runtime on the calling thread, so writing needs no host threads.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_writer_create(
    context: *mut c_void,
    write: VortexFFIWriteCallback,
    schema: *mut FFI_ArrowSchema,
    error: *mut *mut c_char,
) -> *mut VortexFFIWriter {
    unsafe {
        ffi_wrap(error, std::ptr::null_mut(), || {
            let ffi_schema = std::ptr::read(schema);
            let arrow_schema = Schema::try_from(&ffi_schema).map_err(|e| e.to_string())?;
            let runtime = HostRuntime::new(0, None);
            let session = make_session(&runtime);
            let dtype =
                session.arrow().from_arrow_schema(&arrow_schema).map_err(|e| e.to_string())?;
            let sink = CallbackWrite { context: context as usize, write };
            let writer = session.write_options().writer(sink, dtype);
            Ok(Box::into_raw(Box::new(VortexFFIWriter {
                session,
                runtime,
                schema: Arc::new(arrow_schema),
                writer: Some(writer),
            })))
        })
    }
}

/// Appends one record batch (Arrow C Data Interface, consumed) to the file. The batch must have
/// the same schema the writer was created with. Returns 0 on success.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_writer_write(
    writer: *mut VortexFFIWriter,
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

/// Flushes the remaining data and writes the file footer. Must be called exactly once before
/// `vortex_ffi_writer_free`. Returns 0 on success.
#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_writer_finish(
    writer: *mut VortexFFIWriter,
    error: *mut *mut c_char,
) -> i32 {
    unsafe {
        ffi_wrap(error, -1, || {
            let writer = &mut *writer;
            let vortex_writer =
                writer.writer.take().ok_or_else(|| "writer is already finished".to_string())?;
            writer
                .runtime
                .block_on(vortex_writer.finish())
                .map_err(|e| e.to_string())?;
            Ok(0)
        })
    }
}

#[no_mangle]
pub unsafe extern "C" fn vortex_ffi_writer_free(writer: *mut VortexFFIWriter) {
    if !writer.is_null() {
        unsafe { drop(Box::from_raw(writer)) };
    }
}

/// Frees a string returned by this library (for example an error message).
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

    /// An in-memory file that counts the read callback invocations.
    struct TestFile {
        data: Vec<u8>,
        reads: AtomicUsize,
        /// Makes every read fail, to test the I/O error path.
        fail_reads: AtomicBool,
    }

    impl TestFile {
        fn new(data: Vec<u8>) -> Self {
            Self { data, reads: AtomicUsize::new(0), fail_reads: AtomicBool::new(false) }
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
        let Some(end) = offset.checked_add(length) else { return 1 };
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

    /// A host of a runtime: worker threads that run the runtime's queues when notified, like
    /// ClickHouse's thread pools do.
    struct TestHost {
        runtime: *mut VortexFFIRuntime,
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

    unsafe extern "C" fn test_notify(context: *mut c_void, _queue: VortexFFIQueue) {
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
                        let runtime = state.runtime.load(Ordering::Acquire) as *const VortexFFIRuntime;
                        while !state.stop.load(Ordering::Relaxed) {
                            let mut error: *mut c_char = std::ptr::null_mut();
                            let cpu = unsafe {
                                vortex_ffi_runtime_run(runtime, VortexFFIQueue::Cpu, 8, &mut error)
                            };
                            let io = unsafe {
                                vortex_ffi_runtime_run(runtime, VortexFFIQueue::Io, 8, &mut error)
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

            Self { runtime, state, workers }
        }

        fn runtime(&self) -> *const VortexFFIRuntime {
            self.runtime
        }

        /// Stops the worker threads: no task runs afterwards, so the scans and readers on the
        /// runtime can be freed (they must be freed before the runtime is, which `drop` does).
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

    /// Collects the chunks of a scan, like `VortexBlockInputFormat` does.
    struct TestConsumer {
        schema: Mutex<Option<SchemaRef>>,
        chunks: Mutex<Vec<(u64, RecordBatch)>>,
        finished: Mutex<Option<Option<String>>>,
        condvar: Condvar,
        /// Fail the conversion of this split, to test the error path.
        fail_on_split: Option<u64>,
        /// Fail the conversion of every chunk, to test the error path with no chunk to release.
        fail_all: bool,
        /// The chunks that were delivered and not released yet, and the maximum ever seen.
        outstanding: AtomicUsize,
        max_outstanding: AtomicUsize,
    }

    impl TestConsumer {
        fn new(fail_on_split: Option<u64>) -> Arc<Self> {
            Self::with_failures(fail_on_split, false)
        }

        /// A consumer that rejects every chunk.
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

        fn callbacks(self: &Arc<Self>) -> VortexFFIScanConsumer {
            VortexFFIScanConsumer {
                context: Arc::as_ptr(self) as *mut c_void,
                on_chunk: test_on_chunk,
                on_finish: test_on_finish,
            }
        }

        /// Waits for `on_finish` and returns its error message, if any.
        fn wait(&self) -> Option<String> {
            self.wait_for(std::time::Duration::from_secs(60)).expect("the scan did not finish")
        }

        /// Waits for `on_finish` for at most `timeout`: `None` if it was not called in time,
        /// otherwise its error message, if any.
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
        if consumer.fail_all || consumer.fail_on_split == Some(split_index) {
            // The array is still owned by the consumer even when it reports a failure.
            if !array.is_null() {
                drop(unsafe { std::ptr::read(array) });
            }
            return 1;
        }
        if array.is_null() {
            // A split with no rows.
            return 0;
        }
        let outstanding = consumer.outstanding.fetch_add(1, Ordering::Relaxed) + 1;
        consumer.max_outstanding.fetch_max(outstanding, Ordering::Relaxed);

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
            Some(unsafe { CStr::from_ptr(error) }.to_string_lossy().into_owned())
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
            vec![Arc::new(Int64Array::from(ids)), Arc::new(StringArray::from(names))],
        )
        .expect("valid batch")
    }

    fn scan_options() -> VortexFFIScanOptions {
        VortexFFIScanOptions {
            columns: std::ptr::null(),
            num_columns: 0,
            filter: std::ptr::null(),
            row_range_begin: 0,
            row_range_end: 0,
            in_flight: 0,
        }
    }

    fn reader_options(io_concurrency: u32, coalesce: Option<(u64, u64)>) -> VortexFFIReaderOptions {
        let (coalesce_distance, coalesce_max_size) = coalesce.unwrap_or((0, 0));
        VortexFFIReaderOptions { io_concurrency, coalesce_distance, coalesce_max_size }
    }

    /// Opens a reader over `file` with the given options, panicking on error.
    unsafe fn open_reader(
        runtime: *const VortexFFIRuntime,
        file: &mut TestFile,
        options: &VortexFFIReaderOptions,
    ) -> *mut VortexFFIReader {
        let mut error: *mut c_char = std::ptr::null_mut();
        let file_size = file.data.len() as u64;
        let reader = unsafe {
            vortex_ffi_reader_open(runtime, file.context(), read_from_vec, file_size, options, &mut error)
        };
        assert!(!reader.is_null(), "{:?}", unsafe { CStr::from_ptr(error) });
        reader
    }

    /// Writes a file with the given batches into a `Vec<u8>`.
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

    /// The schema of the chunks of a scan with `options`: the file schema projected to the requested
    /// columns. The consumer needs it before the scan is created, as the scan starts producing right
    /// away (this is what `VortexBlockInputFormat` does too).
    unsafe fn expected_scan_schema(reader: *mut VortexFFIReader, options: &VortexFFIScanOptions) -> SchemaRef {
        let mut error: *mut c_char = std::ptr::null_mut();
        let mut ffi_schema = FFI_ArrowSchema::empty();
        assert_eq!(unsafe { vortex_ffi_reader_schema(reader, &mut ffi_schema, &mut error) }, 0);
        let file_schema = Schema::try_from(&ffi_schema).expect("schema");
        if options.columns.is_null() {
            return Arc::new(file_schema);
        }
        let fields: Vec<Field> = (0..options.num_columns as usize)
            .map(|i| {
                let name = unsafe { CStr::from_ptr(*options.columns.add(i)) }.to_str().expect("utf-8 name");
                file_schema.field_with_name(name).expect("a column of the file").clone()
            })
            .collect();
        Arc::new(Schema::new(fields))
    }

    /// Gives `consumer` the schema of the scan, creates the scan and checks that it reports the same
    /// schema, panicking on error.
    unsafe fn start_scan(
        reader: *mut VortexFFIReader,
        options: &VortexFFIScanOptions,
        consumer: &Arc<TestConsumer>,
    ) -> *mut VortexFFIScan {
        let schema = unsafe { expected_scan_schema(reader, options) };
        *consumer.schema.lock().expect("lock") = Some(schema.clone());

        let callbacks = consumer.callbacks();
        let mut error: *mut c_char = std::ptr::null_mut();
        let scan = unsafe { vortex_ffi_scan_create(reader, options, &callbacks, &mut error) };
        assert!(!scan.is_null(), "{:?}", unsafe { CStr::from_ptr(error) });

        let mut ffi_schema = FFI_ArrowSchema::empty();
        assert_eq!(unsafe { vortex_ffi_scan_schema(scan, &mut ffi_schema, &mut error) }, 0);
        assert_eq!(Schema::try_from(&ffi_schema).expect("schema"), *schema);
        scan
    }

    /// Runs a whole scan and returns its consumer.
    unsafe fn run_scan(
        reader: *mut VortexFFIReader,
        options: &VortexFFIScanOptions,
        fail_on_split: Option<u64>,
        release: bool,
    ) -> Arc<TestConsumer> {
        let consumer = TestConsumer::new(fail_on_split);
        let scan = unsafe { start_scan(reader, options, &consumer) };

        if release {
            // Release the chunks as they arrive, like the host does when it returns them.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            loop {
                let released = consumer.outstanding.swap(0, Ordering::Relaxed);
                if released > 0 {
                    unsafe { vortex_ffi_scan_release(scan, released as u64) };
                }
                if consumer.finished.lock().expect("lock").is_some() {
                    break;
                }
                assert!(std::time::Instant::now() < deadline, "the scan did not finish");
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
        consumer.wait();
        unsafe { vortex_ffi_scan_free(scan) };
        consumer
    }

    /// The names of the threads of this process (Linux only; empty elsewhere).
    fn thread_names() -> Vec<String> {
        let Ok(tasks) = std::fs::read_dir("/proc/self/task") else { return Vec::new() };
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
            assert_eq!(vortex_ffi_reader_schema(reader, &mut ffi_schema, &mut error), 0);
            let schema = Schema::try_from(&ffi_schema).expect("schema");
            assert_eq!(schema.field(0).name(), "id");
            assert_eq!(schema.field(1).name(), "name");

            // The whole file.
            let consumer = run_scan(reader, &scan_options(), None, true);
            assert_eq!(consumer.rows(), 5);
            let indices = consumer.split_indices();
            assert!(indices.iter().enumerate().all(|(i, index)| *index == i as u64));

            // A single projected column.
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

            // A pushed-down filter: id > 2 matches rows 3, 4 and 5.
            let id = CString::new("id").expect("valid name");
            let id_column = vortex_ffi_expr_column(id.as_ptr());
            let threshold = vortex_ffi_expr_literal_int(VortexFFIPType::I64, 2);
            let filter = vortex_ffi_expr_compare(VortexFFIComparison::Gt, id_column, threshold);
            assert!(!filter.is_null());
            let mut options = scan_options();
            options.filter = filter;
            let consumer = run_scan(reader, &options, None, true);
            vortex_ffi_expr_free(filter);
            vortex_ffi_expr_free(threshold);
            vortex_ffi_expr_free(id_column);
            assert_eq!(consumer.rows(), 3);

            // A row range.
            let mut options = scan_options();
            options.row_range_begin = 1;
            options.row_range_end = 4;
            let consumer = run_scan(reader, &options, None, true);
            assert_eq!(consumer.rows(), 3);

            // A failing consumer stops the scan and is told why.
            let consumer = run_scan(reader, &scan_options(), Some(0), true);
            let error_message = consumer.wait().expect("the scan must fail");
            assert!(error_message.contains("convert"), "{error_message}");

            vortex_ffi_reader_free(reader);
        }

        // A truncated file must produce an error, not a panic or a crash.
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

    /// A file with many splits, scanned by several host threads: every row arrives exactly once, and
    /// the scan never runs further ahead than the host lets it.
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
            let reader = open_reader(host.runtime(), &mut file, &reader_options(4, Some((1 << 20, 4 << 20))));
            let mut options = scan_options();
            options.in_flight = 3;
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

            // The split indices are dense: the consumer can restore the file order from them.
            let indices = consumer.split_indices();
            assert!(indices.len() > 1, "the file must have several splits");
            assert!(indices.iter().enumerate().all(|(i, index)| *index == i as u64));

            // Never more chunks outstanding than the scan was allowed to have in flight.
            assert!(
                consumer.max_outstanding.load(Ordering::Relaxed) <= options.in_flight as usize,
                "{} chunks were outstanding, in_flight is {}",
                consumer.max_outstanding.load(Ordering::Relaxed),
                options.in_flight
            );

            vortex_ffi_reader_free(reader);
        }
    }

    /// Cancelling a scan stops it without reporting an end to the consumer.
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
            let reader = open_reader(host.runtime(), &mut file, &reader_options(2, Some((1 << 20, 4 << 20))));
            let consumer = TestConsumer::new(None);
            let mut options = scan_options();
            options.in_flight = 2;
            let scan = start_scan(reader, &options, &consumer);

            // Let the scan produce something, then cancel it without releasing anything.
            std::thread::sleep(std::time::Duration::from_millis(50));
            vortex_ffi_scan_cancel(scan);
            // The host stops running the queues before freeing anything the tasks may use, and
            // frees the runtime last.
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

    /// The driver of a scan waits for a permit while the host holds all `in_flight` chunks, and it
    /// must still notice the failure of the split tasks: an I/O error hits every in-flight split at
    /// once (one coalesced read serves several of them), nothing is delivered, so the host has
    /// nothing to release, and the error must reach `on_finish` right away all the same.
    #[test]
    fn ffi_scan_reports_io_error_while_blocked_on_permits() {
        // Incompressible data, so that the file is much larger than the initial footer read
        // (64 KiB) and the segments are actually read through the callback during the scan.
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
        assert!(data.len() > 1 << 20, "test file is only {} bytes", data.len());
        let mut file = TestFile::new(data);

        let mut host = TestHost::new(2);
        unsafe {
            let reader = open_reader(host.runtime(), &mut file, &reader_options(2, Some((1 << 20, 4 << 20))));
            // Every read after the footer fails, i.e. every split task fails.
            file.fail_reads.store(true, Ordering::Relaxed);
            let consumer = TestConsumer::new(None);
            let mut options = scan_options();
            options.in_flight = 4;
            let scan = start_scan(reader, &options, &consumer);

            let outcome = consumer.wait_for(std::time::Duration::from_secs(10));
            vortex_ffi_scan_cancel(scan);
            host.stop();
            let error_message = outcome
                .expect("on_finish was never called after an I/O error")
                .expect("the scan must fail");
            assert!(error_message.contains("read callback failed"), "{error_message}");
            vortex_ffi_scan_free(scan);
            vortex_ffi_reader_free(reader);
        }
    }

    /// The same with the errors coming from the consumer: it rejects every chunk (without cancelling
    /// the scan itself), so the permits of the rejected chunks are never returned, and the driver
    /// must report the first rejection to `on_finish` instead of waiting for a permit forever.
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
            let reader = open_reader(host.runtime(), &mut file, &reader_options(2, Some((1 << 20, 4 << 20))));
            let consumer = TestConsumer::new_failing_all();
            let mut options = scan_options();
            options.in_flight = 2;
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

    /// With coalescing, nearby segments are read with one callback invocation.
    #[test]
    fn ffi_read_coalescing() {
        // Incompressible data, so that the file is much larger than the initial footer read
        // (64 KiB) and the segments are actually read through the callback.
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
        assert!(data.len() > 1 << 20, "test file is only {} bytes", data.len());

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
        assert!(coalesced < plain, "coalesced {coalesced} reads, plain {plain} reads");
    }

    /// The library owns no threads: reading and writing must not leave a reactor (`async-io`) or a
    /// blocking-pool (`blocking-*`) thread behind.
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
                let reader =
                    open_reader(host.runtime(), &mut file, &reader_options(2, Some((1 << 20, 4 << 20))));
                let consumer = run_scan(reader, &scan_options(), None, true);
                assert_eq!(consumer.rows(), 80_000);
                vortex_ffi_reader_free(reader);
            }
        }
        let names = thread_names();
        assert!(
            !names.iter().any(|name| name.starts_with("async-io") || name.starts_with("blocking")),
            "unexpected library threads: {names:?}"
        );
    }

    /// A runtime without a notification callback runs its tasks on the thread of the FFI call, so a
    /// scan can be driven by the host alone, without any worker threads.
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
            let reader = open_reader(runtime, &mut file, &reader_options(1, Some((1 << 20, 4 << 20))));
            let consumer = TestConsumer::new(None);
            let mut options = scan_options();
            options.in_flight = 2;
            let scan = start_scan(reader, &options, &consumer);

            // The only thread there is runs the queues and releases what it received, exactly like
            // `VortexBlockInputFormat` in the single-threaded mode.
            let mut error: *mut c_char = std::ptr::null_mut();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            while consumer.finished.lock().expect("lock").is_none() {
                let cpu = vortex_ffi_runtime_run(runtime, VortexFFIQueue::Cpu, 4, &mut error);
                let io = vortex_ffi_runtime_run(runtime, VortexFFIQueue::Io, 4, &mut error);
                assert!(cpu >= 0 && io >= 0, "a task panicked");
                let released = consumer.outstanding.swap(0, Ordering::Relaxed);
                if released > 0 {
                    vortex_ffi_scan_release(scan, released as u64);
                }
                assert!(std::time::Instant::now() < deadline, "the scan did not finish");
            }
            assert_eq!(consumer.wait(), None);
            assert_eq!(consumer.rows(), total_rows);

            vortex_ffi_scan_free(scan);
            vortex_ffi_reader_free(reader);
            vortex_ffi_runtime_free(runtime);
        }
    }
}
