use arrow_array::ffi::FFI_ArrowArray;
use arrow_array::{Array, StructArray};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::Stream;
use futures::StreamExt;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::ReadParams;
use lance::io::ObjectStoreParams;
use lance::Dataset;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::{ClientOptions, DynObjectStore, RetryConfig};
use sha2::{Digest, Sha256};
use std::any::Any;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::ffi::{CStr, CString};
use std::future::Future;
use std::os::raw::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};
use tokio::sync::Notify;
use url::Url;

#[cfg(not(panic = "unwind"))]
compile_error!(
    "The Lance C ABI requires panic=unwind so every panic is contained by its FFI guard"
);

/// Process-wide multi-thread Tokio runtime shared by all Lance FFI calls.
static LANCE_RUNTIME: OnceLock<Runtime> = OnceLock::new();
/// Worker threads requested before first init; 0 means automatic bounded default.
static LANCE_RUNTIME_WORKER_THREADS: AtomicU32 = AtomicU32::new(0);
static LANCE_RUNTIME_INITIALIZED: AtomicU64 = AtomicU64::new(0);
static LANCE_OPEN_DATASET_CALLS: AtomicU64 = AtomicU64::new(0);
static LANCE_PLAN_SCAN_CALLS: AtomicU64 = AtomicU64::new(0);
static LANCE_NEXT_BATCH_CALLS: AtomicU64 = AtomicU64::new(0);

fn default_worker_threads() -> usize {
    // Bound the pool so concurrent ClickHouse queries cannot spawn N * cores threads.
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    std::cmp::max(2, std::cmp::min(cpus, 8))
}

fn resolve_worker_threads(configured: u32) -> usize {
    if configured == 0 {
        default_worker_threads()
    } else {
        configured as usize
    }
}

fn build_lance_runtime(worker_threads: usize) -> Result<Runtime, String> {
    RuntimeBuilder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .thread_name("lance-tokio")
        .build()
        .map_err(|err| format!("Cannot create Lance runtime: {}", err))
}

fn ensure_lance_runtime() -> Result<&'static Runtime, FfiError> {
    if let Some(runtime) = LANCE_RUNTIME.get() {
        return Ok(runtime);
    }

    let configured = LANCE_RUNTIME_WORKER_THREADS.load(Ordering::Acquire);
    let worker_threads = resolve_worker_threads(configured);
    match build_lance_runtime(worker_threads) {
        Ok(runtime) => match LANCE_RUNTIME.set(runtime) {
            Ok(()) => {
                LANCE_RUNTIME_INITIALIZED.fetch_add(1, Ordering::Relaxed);
                Ok(LANCE_RUNTIME
                    .get()
                    .expect("Lance runtime must be initialized after successful set"))
            }
            Err(_) => Ok(LANCE_RUNTIME
                .get()
                .expect("Lance runtime must be initialized after concurrent set")),
        },
        Err(message) => {
            // Another thread may have succeeded while we failed to build.
            if let Some(runtime) = LANCE_RUNTIME.get() {
                Ok(runtime)
            } else {
                Err(FfiError::internal(ChLanceErrorOrigin::Unknown, message))
            }
        }
    }
}

fn block_on_lance<F>(future: F) -> Result<F::Output, FfiError>
where
    F: Future,
{
    let runtime = ensure_lance_runtime()?;
    Ok(runtime.block_on(future))
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChLanceErrorKind {
    None = 0,
    InvalidArgument = 1,
    NotFound = 2,
    PermissionDenied = 3,
    Unauthenticated = 4,
    CorruptData = 5,
    Unsupported = 6,
    VersionNotFound = 7,
    Storage = 8,
    Internal = 9,
    Cancelled = 10,
    SnapshotMismatch = 11,
}

#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ChLanceErrorOrigin {
    Unknown = 0,
    Local = 1,
    S3 = 2,
}

#[repr(C)]
pub struct ch_lance_error {
    kind: u32,
    origin: u32,
    message: *mut c_char,
}

#[repr(C)]
pub struct ch_lance_dataset_options {
    uri: *const c_char,
    use_s3: bool,
    s3_region: *const c_char,
    s3_endpoint: *const c_char,
    s3_access_key_id: *const c_char,
    s3_secret_access_key: *const c_char,
    s3_session_token: *const c_char,
    s3_role_arn: *const c_char,
    s3_role_session_name: *const c_char,
    s3_use_environment_credentials: bool,
    s3_no_sign_request: bool,
    s3_allow_http: bool,
    s3_virtual_hosted_style_request: bool,
    s3_request_timeout_ms: u64,
    s3_connect_timeout_ms: u64,
    cancel: *mut ch_lance_cancel_handle,
}

const SNAPSHOT_DIGEST_SIZE: usize = 32;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ch_lance_snapshot_info {
    version: u64,
    manifest_id: [u8; SNAPSHOT_DIGEST_SIZE],
    manifest_size: u64,
    manifest_sha256: [u8; SNAPSHOT_DIGEST_SIZE],
    has_etag: bool,
    etag_sha256: [u8; SNAPSHOT_DIGEST_SIZE],
}

#[repr(C)]
pub struct ch_lance_string_list {
    values: *const *const c_char,
    size: usize,
}

#[repr(C)]
pub struct ch_lance_scan_options {
    snapshot: ch_lance_snapshot_info,
    projection: ch_lance_string_list,
    predicate: *const c_char,
    need_only_count: bool,
    max_block_size: u64,
    /// Soft upper bound on rows; 0 means unlimited.
    limit: u64,
    cancel: *mut ch_lance_cancel_handle,
    /// false (zero-init): ordered scan. true: unordered (enables fragment_readahead).
    scan_unordered: bool,
    /// 0 = SDK default; >0 → Scanner::fragment_readahead.
    fragment_readahead: u32,
    /// 0 = SDK default; >0 → Scanner::batch_readahead.
    batch_readahead: u32,
    /// 0 = SDK default; >0 → Scanner::io_buffer_size.
    io_buffer_size: u64,
    /// null or size==0 → all fragments; else restrict with Scanner::with_fragments.
    fragment_ids: *const u64,
    fragment_ids_size: usize,
}

#[repr(C)]
pub struct ch_lance_fragment_info {
    id: u64,
    /// u64::MAX if unknown.
    num_rows: u64,
    /// 0 if unknown.
    size_bytes: u64,
}

#[repr(C)]
pub struct ch_lance_runtime_config {
    worker_threads: u32,
}

#[repr(C)]
pub struct ch_lance_runtime_stats {
    open_dataset_calls: u64,
    plan_scan_calls: u64,
    next_batch_calls: u64,
    runtime_initialized: u64,
}

#[repr(C)]
pub struct ch_lance_dataset {
    dataset: Dataset,
    origin: ChLanceErrorOrigin,
}

/// Cooperative cancel signal. Thread-safe: cancel may be requested from any thread
/// while open/plan/count/next_batch run on a pipeline thread.
struct ScanCancel {
    cancelled: AtomicBool,
    notify: Notify,
}

impl ScanCancel {
    fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    fn cancel(&self) {
        // Release store so a subsequent Acquire load observes the flag.
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Opaque query-scoped cancel token shared across open/plan/count/scan.
#[allow(non_camel_case_types)]
pub struct ch_lance_cancel_handle {
    inner: Arc<ScanCancel>,
}

/// Resolve an optional FFI cancel handle into an Arc. Null creates a private token
/// (scan-only cancel via ch_lance_cancel_scan still works).
unsafe fn cancel_arc_from_ptr(ptr: *mut ch_lance_cancel_handle) -> Arc<ScanCancel> {
    if ptr.is_null() {
        Arc::new(ScanCancel::new())
    } else {
        (*ptr).inner.clone()
    }
}

unsafe fn optional_cancel_from_ptr(ptr: *mut ch_lance_cancel_handle) -> Option<Arc<ScanCancel>> {
    if ptr.is_null() {
        None
    } else {
        Some((*ptr).inner.clone())
    }
}

type LanceBatchStream = Pin<Box<dyn Stream<Item = lance::Result<arrow_array::RecordBatch>> + Send>>;

/// Opaque to C; only ever manipulated via FFI entry points.
///
/// `stream` is behind a mutex so `ch_lance_cancel_scan` may run concurrently with
/// `ch_lance_next_batch` without taking an exclusive borrow of the whole struct.
/// Cancel only touches `cancel` (Arc atomics + Notify); stream drop on cancel happens
/// inside `next_batch` (or `free_scan`) while the mutex is held.
#[allow(non_camel_case_types)]
pub struct ch_lance_scan {
    /// Taken (set to None) when cancelled or fully consumed so drop cancels Lance I/O queue.
    stream: Mutex<Option<LanceBatchStream>>,
    origin: ChLanceErrorOrigin,
    cancel: Arc<ScanCancel>,
}

/// Race a one-shot future against cooperative cancellation.
async fn with_cancel<T, F>(cancel: Option<&ScanCancel>, fut: F) -> Result<T, FfiError>
where
    F: Future<Output = Result<T, FfiError>>,
{
    let Some(cancel) = cancel else {
        return fut.await;
    };

    if cancel.is_cancelled() {
        return Err(FfiError::cancelled());
    }

    tokio::pin!(fut);
    loop {
        let notified = cancel.notify.notified();
        tokio::pin!(notified);

        if cancel.is_cancelled() {
            return Err(FfiError::cancelled());
        }

        tokio::select! {
            biased;
            _ = &mut notified => {
                if cancel.is_cancelled() {
                    return Err(FfiError::cancelled());
                }
            }
            result = &mut fut => {
                return result;
            }
        }
    }
}

/// Wait for the next stream item or cooperative cancellation.
///
/// On cancel, returns `Err` with Cancelled without dropping the stream; the caller must
/// `take()` the stream while holding exclusive access to `ch_lance_scan`.
async fn next_batch_or_cancel(
    stream: &mut LanceBatchStream,
    cancel: &ScanCancel,
) -> Result<Option<lance::Result<arrow_array::RecordBatch>>, FfiError> {
    loop {
        if cancel.is_cancelled() {
            return Err(FfiError::cancelled());
        }

        // Subscribe before re-checking the flag so a cancel between the check and
        // select cannot be lost (standard Notify pattern).
        let notified = cancel.notify.notified();
        tokio::pin!(notified);

        if cancel.is_cancelled() {
            return Err(FfiError::cancelled());
        }

        tokio::select! {
            biased;
            _ = &mut notified => {
                if cancel.is_cancelled() {
                    return Err(FfiError::cancelled());
                }
                // Spurious wake: retry.
            }
            item = stream.next() => {
                return Ok(item);
            }
        }
    }
}

#[derive(Clone, Copy)]
enum LanceOperation {
    Open,
    CheckoutVersion,
    CountRows,
    PlanScan,
    NextBatch,
}

impl LanceOperation {
    fn description(self) -> &'static str {
        match self {
            Self::Open => "Cannot open Lance dataset",
            Self::CheckoutVersion => "Cannot check out Lance dataset version",
            Self::CountRows => "Cannot count Lance rows",
            Self::PlanScan => "Cannot plan Lance scan",
            Self::NextBatch => "Cannot read Lance record batch",
        }
    }
}

fn list_fragment_infos(dataset: &Dataset) -> Vec<ch_lance_fragment_info> {
    dataset
        .fragments()
        .iter()
        .map(|fragment| ch_lance_fragment_info {
            id: fragment.id,
            num_rows: fragment.num_rows().map(|n| n as u64).unwrap_or(u64::MAX),
            size_bytes: fragment
                .files
                .iter()
                .map(|file| file.file_size_bytes.get().map(|n| n.get()).unwrap_or(0))
                .sum(),
        })
        .collect()
}

#[derive(Debug)]
struct FfiError {
    kind: ChLanceErrorKind,
    origin: ChLanceErrorOrigin,
    message: String,
}

type FfiResult<T> = Result<T, FfiError>;

impl FfiError {
    fn new(kind: ChLanceErrorKind, origin: ChLanceErrorOrigin, message: impl Into<String>) -> Self {
        Self {
            kind,
            origin,
            message: redact_uri_user_info(message.into()),
        }
    }

    fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(
            ChLanceErrorKind::InvalidArgument,
            ChLanceErrorOrigin::Unknown,
            message,
        )
    }

    fn unsupported(message: impl Into<String>) -> Self {
        Self::new(
            ChLanceErrorKind::Unsupported,
            ChLanceErrorOrigin::Unknown,
            message,
        )
    }

    fn internal(origin: ChLanceErrorOrigin, message: impl Into<String>) -> Self {
        Self::new(ChLanceErrorKind::Internal, origin, message)
    }

    fn cancelled() -> Self {
        Self::new(
            ChLanceErrorKind::Cancelled,
            ChLanceErrorOrigin::Unknown,
            "Lance scan was cancelled",
        )
    }

    fn from_lance(
        operation: LanceOperation,
        origin: ChLanceErrorOrigin,
        error: lance::Error,
    ) -> Self {
        let kind = classify_lance_error(&error, operation);
        Self::new(
            kind,
            origin,
            format!("{}: {}", operation.description(), error),
        )
    }

    fn from_object_store(origin: ChLanceErrorOrigin, error: object_store::Error) -> Self {
        let kind = classify_object_store_error(&error, LanceOperation::Open);
        Self::new(kind, origin, error.to_string())
    }

    fn with_origin(mut self, origin: ChLanceErrorOrigin) -> Self {
        self.origin = origin;
        self
    }

    fn with_context(mut self, context: impl std::fmt::Display) -> Self {
        self.message = redact_uri_user_info(format!("{}: {}", context, self.message));
        self
    }
}

fn redact_uri_user_info(mut message: String) -> String {
    let mut search_from = 0;
    while let Some(relative_scheme_end) = message[search_from..].find("://") {
        let authority_start = search_from + relative_scheme_end + 3;
        let authority_end = message[authority_start..]
            .find(|character: char| {
                character == '/'
                    || character.is_whitespace()
                    || matches!(character, ')' | ']' | '>')
            })
            .map_or(message.len(), |offset| authority_start + offset);

        let Some(relative_at) = message[authority_start..authority_end].rfind('@') else {
            search_from = authority_end;
            continue;
        };
        let user_info_end = authority_start + relative_at;
        message.replace_range(authority_start..user_info_end, "<redacted>");
        search_from = authority_start + "<redacted>@".len();
    }
    message
}

fn classify_object_store_error(
    error: &object_store::Error,
    operation: LanceOperation,
) -> ChLanceErrorKind {
    match error {
        object_store::Error::NotFound { .. } => ChLanceErrorKind::NotFound,
        object_store::Error::InvalidPath { .. }
        | object_store::Error::UnknownConfigurationKey { .. } => ChLanceErrorKind::InvalidArgument,
        object_store::Error::PermissionDenied { .. } => ChLanceErrorKind::PermissionDenied,
        object_store::Error::Unauthenticated { .. } => ChLanceErrorKind::Unauthenticated,
        object_store::Error::NotSupported { .. } | object_store::Error::NotImplemented => {
            ChLanceErrorKind::Unsupported
        }
        object_store::Error::Generic { source, .. } => {
            classify_error_source(source.as_ref(), operation).unwrap_or(ChLanceErrorKind::Storage)
        }
        _ => ChLanceErrorKind::Storage,
    }
}

fn classify_error_source(
    source: &(dyn StdError + 'static),
    operation: LanceOperation,
) -> Option<ChLanceErrorKind> {
    if let Some(error) = source.downcast_ref::<lance::Error>() {
        return Some(classify_lance_error(error, operation));
    }
    if let Some(error) = source.downcast_ref::<object_store::Error>() {
        return Some(classify_object_store_error(error, operation));
    }
    if source.downcast_ref::<url::ParseError>().is_some()
        || source.downcast_ref::<object_store::path::Error>().is_some()
    {
        return Some(ChLanceErrorKind::InvalidArgument);
    }
    if source.downcast_ref::<prost::DecodeError>().is_some()
        || source.downcast_ref::<prost::UnknownEnumValue>().is_some()
    {
        return Some(ChLanceErrorKind::CorruptData);
    }
    if source.downcast_ref::<tokio::task::JoinError>().is_some() {
        return Some(ChLanceErrorKind::Internal);
    }
    if let Some(inner) = source.source() {
        if let Some(kind) = classify_error_source(inner, operation) {
            return Some(kind);
        }
    }
    if let Some(error) = source.downcast_ref::<std::io::Error>() {
        return Some(match error.kind() {
            std::io::ErrorKind::NotFound => ChLanceErrorKind::NotFound,
            std::io::ErrorKind::PermissionDenied => ChLanceErrorKind::PermissionDenied,
            std::io::ErrorKind::InvalidInput => ChLanceErrorKind::InvalidArgument,
            std::io::ErrorKind::InvalidData => ChLanceErrorKind::CorruptData,
            std::io::ErrorKind::Unsupported => ChLanceErrorKind::Unsupported,
            _ => ChLanceErrorKind::Storage,
        });
    }
    None
}

fn classify_dataset_not_found(
    source: &(dyn StdError + 'static),
    operation: LanceOperation,
) -> ChLanceErrorKind {
    classify_error_source(source, operation).unwrap_or(ChLanceErrorKind::NotFound)
}

fn classify_lance_error(error: &lance::Error, operation: LanceOperation) -> ChLanceErrorKind {
    match error {
        lance::Error::InvalidInput { .. }
        | lance::Error::InvalidTableLocation { .. }
        | lance::Error::InvalidRef { .. }
        | lance::Error::DatasetAlreadyExists { .. } => ChLanceErrorKind::InvalidArgument,
        lance::Error::DatasetNotFound { source, .. } => {
            classify_dataset_not_found(source.as_ref(), operation)
        }
        lance::Error::NotFound { .. }
        | lance::Error::IndexNotFound { .. }
        | lance::Error::RefNotFound { .. } => ChLanceErrorKind::NotFound,
        lance::Error::CorruptFile { .. }
        | lance::Error::SchemaMismatch { .. }
        | lance::Error::Unprocessable { .. }
        | lance::Error::Arrow { .. }
        | lance::Error::Schema { .. } => ChLanceErrorKind::CorruptData,
        lance::Error::NotSupported { .. } => ChLanceErrorKind::Unsupported,
        lance::Error::VersionNotFound { .. } => ChLanceErrorKind::VersionNotFound,
        lance::Error::IO { source, .. } => {
            classify_error_source(source.as_ref(), operation).unwrap_or(ChLanceErrorKind::Storage)
        }
        lance::Error::Wrapped { error, .. } => {
            classify_error_source(error.as_ref(), operation).unwrap_or(ChLanceErrorKind::Internal)
        }
        lance::Error::Namespace { source, .. } | lance::Error::External { source } => {
            classify_error_source(source.as_ref(), operation).unwrap_or(ChLanceErrorKind::Internal)
        }
        _ => ChLanceErrorKind::Internal,
    }
}

fn set_error(error: *mut ch_lance_error, ffi_error: FfiError) {
    if error.is_null() {
        return;
    }

    clear_error(error);
    let message = CString::new(ffi_error.message)
        .unwrap_or_else(|_| CString::new("Lance error contains an interior null byte").unwrap());
    unsafe {
        (*error).kind = ffi_error.kind as u32;
        (*error).origin = ffi_error.origin as u32;
        (*error).message = message.into_raw();
    }
}

fn clear_error(error: *mut ch_lance_error) {
    if !error.is_null() {
        unsafe {
            if !(*error).message.is_null() {
                drop(CString::from_raw((*error).message));
            }
            (*error).kind = ChLanceErrorKind::None as u32;
            (*error).origin = ChLanceErrorOrigin::Unknown as u32;
            (*error).message = ptr::null_mut();
        }
    }
}

fn cstr_to_string(ptr: *const c_char) -> FfiResult<String> {
    if ptr.is_null() {
        return Ok(String::new());
    }

    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map(|s| s.to_string())
        .map_err(|err| FfiError::invalid_argument(err.to_string()))
}

fn required_cstr_to_string(ptr: *const c_char, name: &str) -> FfiResult<String> {
    let value = cstr_to_string(ptr)?;
    if value.is_empty() {
        Err(FfiError::invalid_argument(format!(
            "Lance dataset option `{}` must not be empty",
            name
        )))
    } else {
        Ok(value)
    }
}

#[derive(Clone)]
struct DatasetOpenOptions {
    uri: String,
    storage_options: Option<HashMap<String, String>>,
    origin: ChLanceErrorOrigin,
}

struct OpenedDataset {
    dataset: Dataset,
    origin: ChLanceErrorOrigin,
}

unsafe fn apply_dataset_options(
    options: &ch_lance_dataset_options,
) -> FfiResult<DatasetOpenOptions> {
    let origin = if options.use_s3 {
        ChLanceErrorOrigin::S3
    } else {
        ChLanceErrorOrigin::Local
    };
    let uri = required_cstr_to_string(options.uri, "uri").map_err(|err| err.with_origin(origin))?;
    let storage_options = if options.use_s3 {
        let mut values = HashMap::new();

        let fields = [
            ("aws_region", options.s3_region),
            ("aws_endpoint", options.s3_endpoint),
            ("aws_access_key_id", options.s3_access_key_id),
            ("aws_secret_access_key", options.s3_secret_access_key),
            ("aws_session_token", options.s3_session_token),
            ("aws_role_arn", options.s3_role_arn),
            ("aws_role_session_name", options.s3_role_session_name),
        ];
        for (name, value_ptr) in fields {
            let value = cstr_to_string(value_ptr).map_err(|err| err.with_origin(origin))?;
            if !value.is_empty() {
                values.insert(name.to_string(), value);
            }
        }
        if options.s3_no_sign_request {
            values.insert("aws_skip_signature".to_string(), "true".to_string());
        }
        if options.s3_use_environment_credentials {
            values.insert(
                "aws_use_environment_credentials".to_string(),
                "true".to_string(),
            );
        }
        if options.s3_allow_http {
            values.insert("aws_allow_http".to_string(), "true".to_string());
        }
        values.insert(
            "aws_virtual_hosted_style_request".to_string(),
            if options.s3_virtual_hosted_style_request {
                "true"
            } else {
                "false"
            }
            .to_string(),
        );
        // HTTP deadlines (Phase C). Keys match object_store ClientConfigKey humantime strings.
        if options.s3_request_timeout_ms != 0 {
            values.insert(
                "timeout".to_string(),
                format!("{}ms", options.s3_request_timeout_ms),
            );
        }
        if options.s3_connect_timeout_ms != 0 {
            values.insert(
                "connect_timeout".to_string(),
                format!("{}ms", options.s3_connect_timeout_ms),
            );
        }
        Some(values)
    } else {
        None
    };
    Ok(DatasetOpenOptions {
        uri,
        storage_options,
        origin,
    })
}

async fn open_dataset(options: DatasetOpenOptions) -> FfiResult<OpenedDataset> {
    let DatasetOpenOptions {
        uri,
        storage_options,
        origin,
    } = options;
    if let Some(storage_options) = storage_options {
        let object_store = build_s3_store(&uri, &storage_options)?;
        let location = Url::parse(&uri).map_err(|err| {
            FfiError::new(ChLanceErrorKind::InvalidArgument, origin, err.to_string())
        })?;
        #[allow(deprecated)]
        let store_options = ObjectStoreParams {
            object_store: Some((object_store, location)),
            ..Default::default()
        };
        let dataset = DatasetBuilder::from_uri(&uri)
            .with_read_params(ReadParams {
                store_options: Some(store_options),
                ..Default::default()
            })
            .load()
            .await
            .map_err(|err| FfiError::from_lance(LanceOperation::Open, origin, err))?;
        Ok(OpenedDataset { dataset, origin })
    } else {
        let dataset = Dataset::open(&uri)
            .await
            .map_err(|err| FfiError::from_lance(LanceOperation::Open, origin, err))?;
        Ok(OpenedDataset { dataset, origin })
    }
}

fn digest_bytes(bytes: &[u8]) -> [u8; SNAPSHOT_DIGEST_SIZE] {
    Sha256::digest(bytes).into()
}

fn snapshot_etag_identity(etag: Option<&str>) -> (bool, [u8; SNAPSHOT_DIGEST_SIZE]) {
    match etag {
        Some(etag) => (true, digest_bytes(etag.as_bytes())),
        None => (false, [0; SNAPSHOT_DIGEST_SIZE]),
    }
}

fn validate_snapshot(snapshot: &ch_lance_snapshot_info) -> FfiResult<()> {
    if snapshot.version == 0 {
        return Err(FfiError::invalid_argument(
            "Lance dataset snapshot version must be non-zero",
        ));
    }
    if snapshot.manifest_size == 0 {
        return Err(FfiError::invalid_argument(
            "Lance dataset snapshot manifest size must be non-zero",
        ));
    }
    if snapshot.manifest_id.iter().all(|byte| *byte == 0) {
        return Err(FfiError::invalid_argument(
            "Lance dataset snapshot manifest ID must be non-zero",
        ));
    }
    if snapshot.manifest_sha256.iter().all(|byte| *byte == 0) {
        return Err(FfiError::invalid_argument(
            "Lance dataset snapshot manifest SHA-256 must be non-zero",
        ));
    }
    let etag_is_zero = snapshot.etag_sha256.iter().all(|byte| *byte == 0);
    if snapshot.has_etag == etag_is_zero {
        return Err(FfiError::invalid_argument(
            "Lance dataset snapshot has an invalid e_tag digest state",
        ));
    }
    Ok(())
}

unsafe fn snapshot_from_ffi(
    snapshot: *const ch_lance_snapshot_info,
    origin: ChLanceErrorOrigin,
) -> FfiResult<ch_lance_snapshot_info> {
    if snapshot.is_null() {
        return Err(
            FfiError::invalid_argument("Lance dataset snapshot pointer is null")
                .with_origin(origin),
        );
    }
    let snapshot = *snapshot;
    validate_snapshot(&snapshot).map_err(|error| error.with_origin(origin))?;
    Ok(snapshot)
}

async fn snapshot_identity(
    dataset: &Dataset,
    origin: ChLanceErrorOrigin,
) -> FfiResult<ch_lance_snapshot_info> {
    let location = dataset.manifest_location();
    if location.version == 0 {
        return Err(FfiError::internal(
            origin,
            "Lance returned zero as the current dataset version",
        ));
    }

    let manifest_bytes = dataset
        .object_store()
        .read_one_all(&location.path)
        .await
        .map_err(|err| {
            FfiError::from_lance(LanceOperation::CheckoutVersion, origin, err)
                .with_context(format!("Cannot read Lance manifest {}", location.path))
        })?;
    let manifest_size = u64::try_from(manifest_bytes.len())
        .map_err(|_| FfiError::internal(origin, "Lance manifest size does not fit in uint64"))?;
    if manifest_size == 0 {
        return Err(FfiError::new(
            ChLanceErrorKind::CorruptData,
            origin,
            format!("Lance manifest {} is empty", location.path),
        ));
    }
    if let Some(expected_size) = location.size {
        if expected_size != manifest_size {
            return Err(FfiError::new(
                ChLanceErrorKind::CorruptData,
                origin,
                format!(
                    "Lance manifest {} size mismatch: metadata {}, bytes {}",
                    location.path, expected_size, manifest_size
                ),
            ));
        }
    }

    let scheme = match format!("{:?}", location.naming_scheme).as_str() {
        "V1" => 1_u8,
        "V2" => 2_u8,
        _ => {
            return Err(FfiError::internal(
                origin,
                "Lance returned an unknown manifest naming scheme",
            ))
        }
    };
    let mut manifest_id_hasher = Sha256::new();
    manifest_id_hasher.update(location.path.as_ref().as_bytes());
    manifest_id_hasher.update([0, scheme]);
    let manifest_id = manifest_id_hasher.finalize().into();
    let manifest_sha256 = digest_bytes(&manifest_bytes);
    let (has_etag, etag_sha256) = snapshot_etag_identity(location.e_tag.as_deref());

    let snapshot = ch_lance_snapshot_info {
        version: location.version,
        manifest_id,
        manifest_size,
        manifest_sha256,
        has_etag,
        etag_sha256,
    };
    validate_snapshot(&snapshot).map_err(|error| error.with_origin(origin))?;
    Ok(snapshot)
}

async fn checkout_exact_snapshot(
    dataset: &Dataset,
    expected: ch_lance_snapshot_info,
    origin: ChLanceErrorOrigin,
) -> FfiResult<Dataset> {
    validate_snapshot(&expected).map_err(|error| error.with_origin(origin))?;

    let checked_out = if expected.version == dataset.version().version {
        dataset.clone()
    } else {
        dataset
            .checkout_version(expected.version)
            .await
            .map_err(|err| {
                let mut ffi_error =
                    FfiError::from_lance(LanceOperation::CheckoutVersion, origin, err);
                if ffi_error.kind == ChLanceErrorKind::NotFound {
                    ffi_error.kind = ChLanceErrorKind::VersionNotFound;
                }
                ffi_error.with_context(format!(
                    "Requested Lance dataset version {}",
                    expected.version
                ))
            })?
    };

    let actual = snapshot_identity(&checked_out, origin)
        .await
        .map_err(|error| {
            if error.kind == ChLanceErrorKind::CorruptData {
                FfiError::new(
                    ChLanceErrorKind::SnapshotMismatch,
                    origin,
                    format!(
                        "Lance snapshot identity mismatch for manifest {} at version {}",
                        checked_out.manifest_location().path,
                        expected.version
                    ),
                )
            } else {
                error
            }
        })?;
    if actual != expected {
        return Err(FfiError::new(
            ChLanceErrorKind::SnapshotMismatch,
            origin,
            format!(
                "Lance snapshot identity mismatch for manifest {} at version {}",
                checked_out.manifest_location().path,
                expected.version
            ),
        ));
    }
    Ok(checked_out)
}

fn build_s3_store(
    uri: &str,
    storage_options: &HashMap<String, String>,
) -> FfiResult<Arc<DynObjectStore>> {
    let mut builder = if storage_options
        .get("aws_use_environment_credentials")
        .is_some_and(|value| value == "true")
    {
        AmazonS3Builder::from_env()
    } else {
        AmazonS3Builder::new()
    }
    .with_url(uri);

    // Explicit ClientOptions so timeouts are applied even when only one is set.
    let mut client_options = ClientOptions::new();
    let mut has_client_options = false;
    if let Some(timeout) = storage_options.get("timeout") {
        client_options =
            client_options.with_timeout(parse_timeout_duration(timeout).map_err(|err| {
                FfiError::invalid_argument(err).with_origin(ChLanceErrorOrigin::S3)
            })?);
        has_client_options = true;
    }
    if let Some(connect_timeout) = storage_options.get("connect_timeout") {
        client_options =
            client_options.with_connect_timeout(parse_timeout_duration(connect_timeout).map_err(
                |err| FfiError::invalid_argument(err).with_origin(ChLanceErrorOrigin::S3),
            )?);
        has_client_options = true;
    }
    if has_client_options {
        builder = builder.with_client_options(client_options);
    }

    // Cap overall retry budget so a flaky/hanging endpoint cannot retry for minutes.
    // object_store defaults: max_retries=10, retry_timeout=180s.
    if let Some(timeout) = storage_options.get("timeout") {
        let request_timeout = parse_timeout_duration(timeout)
            .map_err(|err| FfiError::invalid_argument(err).with_origin(ChLanceErrorOrigin::S3))?;
        let retry_timeout = request_timeout
            .checked_mul(3)
            .unwrap_or(request_timeout)
            .max(request_timeout);
        builder = builder.with_retry(RetryConfig {
            max_retries: 3,
            retry_timeout,
            ..Default::default()
        });
    }

    for (key, value) in storage_options {
        // Already applied above; skip to avoid double-setting or unknown-key noise.
        if key == "timeout" || key == "connect_timeout" || key == "aws_use_environment_credentials"
        {
            continue;
        }
        if let Ok(config_key) = key.parse::<AmazonS3ConfigKey>() {
            builder = builder.with_config(config_key, value);
        }
    }
    builder
        .build()
        .map(|store| Arc::new(store) as Arc<DynObjectStore>)
        .map_err(|err| FfiError::from_object_store(ChLanceErrorOrigin::S3, err))
}

fn parse_timeout_duration(value: &str) -> Result<Duration, String> {
    // We emit `{n}ms` from FFI; also accept `{n}s` and bare milliseconds.
    let trimmed = value.trim();
    if let Some(ms_str) = trimmed.strip_suffix("ms") {
        let ms: u64 = ms_str.trim().parse().map_err(|_| {
            format!(
                "Invalid S3 timeout `{}`: expected duration like `30000ms`",
                value
            )
        })?;
        return Ok(Duration::from_millis(ms));
    }
    if let Some(s_str) = trimmed.strip_suffix('s') {
        // Avoid matching the trailing 's' of "ms" (handled above).
        let secs: u64 = s_str.trim().parse().map_err(|_| {
            format!(
                "Invalid S3 timeout `{}`: expected duration like `30s`",
                value
            )
        })?;
        return Ok(Duration::from_secs(secs));
    }
    if let Ok(ms) = trimmed.parse::<u64>() {
        return Ok(Duration::from_millis(ms));
    }
    Err(format!(
        "Invalid S3 timeout `{}`: expected `30s`, `30000ms`, or integer milliseconds",
        value
    ))
}

fn projection_from_ffi(list: &ch_lance_string_list) -> FfiResult<Vec<String>> {
    if list.size == 0 {
        return Ok(Vec::new());
    }
    if list.values.is_null() {
        return Err(FfiError::invalid_argument(
            "Lance projection list is null but size is non-zero",
        ));
    }

    let mut result = Vec::with_capacity(list.size);
    for index in 0..list.size {
        let value_ptr = unsafe { *list.values.add(index) };
        result.push(required_cstr_to_string(value_ptr, "projection")?);
    }
    Ok(result)
}

unsafe fn fragment_ids_from_ffi(
    fragment_ids: *const u64,
    fragment_ids_size: usize,
) -> FfiResult<Option<Vec<u64>>> {
    if fragment_ids_size == 0 {
        return Ok(None);
    }
    if fragment_ids.is_null() {
        return Err(FfiError::invalid_argument(
            "Lance fragment id list is null but size is non-zero",
        ));
    }
    Ok(Some(
        std::slice::from_raw_parts(fragment_ids, fragment_ids_size).to_vec(),
    ))
}

fn write_schema(
    schema: arrow_schema::Schema,
    out: *mut FFI_ArrowSchema,
    origin: ChLanceErrorOrigin,
) -> FfiResult<()> {
    if out.is_null() {
        return Err(
            FfiError::invalid_argument("ArrowSchema output pointer is null").with_origin(origin),
        );
    }

    let ffi_schema = FFI_ArrowSchema::try_from(&schema).map_err(|err| {
        FfiError::internal(
            origin,
            format!(
                "Cannot export Lance schema through Arrow C Data Interface: {}",
                err
            ),
        )
    })?;
    unsafe {
        std::ptr::write_unaligned(out, ffi_schema);
    }
    Ok(())
}

fn validate_schema(schema: &Schema, origin: ChLanceErrorOrigin) -> FfiResult<()> {
    for field in schema.fields() {
        validate_field(field, field.name(), origin)?;
    }
    Ok(())
}

fn unsupported_column(
    column_path: &str,
    message: impl std::fmt::Display,
    origin: ChLanceErrorOrigin,
) -> FfiError {
    FfiError::unsupported(format!(
        "Unsupported Lance column `{}`: {}",
        column_path, message
    ))
    .with_origin(origin)
}

fn validate_field(field: &Field, column_path: &str, origin: ChLanceErrorOrigin) -> FfiResult<()> {
    if field.metadata().contains_key("ARROW:extension:name") {
        return Err(unsupported_column(
            column_path,
            "Arrow extension types are not supported",
            origin,
        ));
    }
    validate_data_type(column_path, field.data_type(), origin)
}

fn validate_data_type(
    column_path: &str,
    data_type: &DataType,
    origin: ChLanceErrorOrigin,
) -> FfiResult<()> {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Date32
        | DataType::Timestamp(TimeUnit::Second, _)
        | DataType::Timestamp(TimeUnit::Millisecond, _)
        | DataType::Timestamp(TimeUnit::Microsecond, _)
        | DataType::Timestamp(TimeUnit::Nanosecond, _)
        | DataType::Duration(TimeUnit::Second)
        | DataType::Duration(TimeUnit::Millisecond)
        | DataType::Duration(TimeUnit::Microsecond)
        | DataType::Duration(TimeUnit::Nanosecond) => Ok(()),
        DataType::Time32(TimeUnit::Second)
        | DataType::Time32(TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond)
        | DataType::Time64(TimeUnit::Nanosecond) => Ok(()),
        DataType::FixedSizeBinary(width) if *width > 0 => Ok(()),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            if *scale < 0 || *scale as u8 > *precision {
                return Err(unsupported_column(
                    column_path,
                    format!(
                        "decimal scale {} must be between 0 and precision {}",
                        scale, precision
                    ),
                    origin,
                ));
            }
            Ok(())
        }
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => {
            validate_field(child, &format!("{}[]", column_path), origin)
        }
        DataType::Struct(fields) => {
            for field in fields {
                validate_field(field, &format!("{}.{}", column_path, field.name()), origin)?;
            }
            Ok(())
        }
        DataType::Map(entries, _) => {
            if entries.metadata().contains_key("ARROW:extension:name") {
                return Err(unsupported_column(
                    column_path,
                    "Arrow extension types are not supported",
                    origin,
                ));
            }

            let DataType::Struct(fields) = entries.data_type() else {
                return Err(unsupported_column(
                    column_path,
                    "map entries must use an Arrow struct",
                    origin,
                ));
            };
            if fields.len() != 2 {
                return Err(unsupported_column(
                    column_path,
                    format!(
                        "map entries struct must have two fields, got {}",
                        fields.len()
                    ),
                    origin,
                ));
            }

            let key = &fields[0];
            let value = &fields[1];
            if key.is_nullable() {
                return Err(unsupported_column(
                    &format!("{}.key", column_path),
                    "map keys must not be nullable",
                    origin,
                ));
            }
            validate_field(key, &format!("{}.key", column_path), origin)?;
            validate_field(value, &format!("{}.value", column_path), origin)
        }
        DataType::Date64 => Err(unsupported_column(
            column_path,
            "Arrow Date64 does not have a lossless ClickHouse mapping",
            origin,
        )),
        DataType::FixedSizeBinary(width) => Err(unsupported_column(
            column_path,
            format!("fixed-size binary width {} must be positive", width),
            origin,
        )),
        DataType::Time32(unit) => Err(unsupported_column(
            column_path,
            format!("Arrow Time32 unit {:?} is not supported", unit),
            origin,
        )),
        DataType::Time64(unit) => Err(unsupported_column(
            column_path,
            format!("Arrow Time64 unit {:?} is not supported", unit),
            origin,
        )),
        other => Err(unsupported_column(
            column_path,
            format!("Arrow type {} is not supported", other),
            origin,
        )),
    }
}

fn write_record_batch(
    batch: arrow_array::RecordBatch,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    origin: ChLanceErrorOrigin,
) -> FfiResult<()> {
    if array.is_null() {
        return Err(
            FfiError::invalid_argument("ArrowArray output pointer is null").with_origin(origin),
        );
    }
    if schema.is_null() {
        return Err(
            FfiError::invalid_argument("ArrowSchema output pointer is null").with_origin(origin),
        );
    }

    validate_schema(batch.schema().as_ref(), origin)?;
    let ffi_schema = FFI_ArrowSchema::try_from(batch.schema().as_ref()).map_err(|err| {
        FfiError::internal(
            origin,
            format!(
                "Cannot export Lance record batch schema through Arrow C Data Interface: {}",
                err
            ),
        )
    })?;
    let struct_array = StructArray::from(batch);
    let ffi_array = FFI_ArrowArray::new(&struct_array.to_data());

    unsafe {
        std::ptr::write_unaligned(array, ffi_array);
        std::ptr::write_unaligned(schema, ffi_schema);
    }
    Ok(())
}

unsafe fn ch_lance_runtime_ensure_impl(
    config: *const ch_lance_runtime_config,
    error: *mut ch_lance_error,
) -> bool {
    clear_error(error);
    if !config.is_null() && LANCE_RUNTIME.get().is_none() {
        let worker_threads = (*config).worker_threads;
        LANCE_RUNTIME_WORKER_THREADS.store(worker_threads, Ordering::Release);
    }
    match ensure_lance_runtime() {
        Ok(_) => true,
        Err(ffi_error) => {
            set_error(error, ffi_error);
            false
        }
    }
}

unsafe fn ch_lance_get_runtime_stats_impl(out: *mut ch_lance_runtime_stats) {
    if out.is_null() {
        return;
    }
    (*out).open_dataset_calls = LANCE_OPEN_DATASET_CALLS.load(Ordering::Relaxed);
    (*out).plan_scan_calls = LANCE_PLAN_SCAN_CALLS.load(Ordering::Relaxed);
    (*out).next_batch_calls = LANCE_NEXT_BATCH_CALLS.load(Ordering::Relaxed);
    (*out).runtime_initialized = LANCE_RUNTIME_INITIALIZED.load(Ordering::Relaxed);
}

unsafe fn ch_lance_open_dataset_impl(
    options: *const ch_lance_dataset_options,
    error: *mut ch_lance_error,
) -> *mut ch_lance_dataset {
    clear_error(error);
    if options.is_null() {
        set_error(
            error,
            FfiError::invalid_argument("Lance dataset options pointer is null"),
        );
        return std::ptr::null_mut();
    }

    let open_options = match apply_dataset_options(&*options) {
        Ok(options) => options,
        Err(ffi_error) => {
            set_error(error, ffi_error);
            return std::ptr::null_mut();
        }
    };

    let cancel = unsafe { optional_cancel_from_ptr((*options).cancel) };

    LANCE_OPEN_DATASET_CALLS.fetch_add(1, Ordering::Relaxed);
    match block_on_lance(with_cancel(cancel.as_deref(), open_dataset(open_options))) {
        Ok(Ok(opened)) => Box::into_raw(Box::new(ch_lance_dataset {
            dataset: opened.dataset,
            origin: opened.origin,
        })),
        Ok(Err(ffi_error)) => {
            set_error(error, ffi_error);
            std::ptr::null_mut()
        }
        Err(ffi_error) => {
            set_error(error, ffi_error);
            std::ptr::null_mut()
        }
    }
}

unsafe fn ch_lance_cancel_handle_create_impl() -> *mut ch_lance_cancel_handle {
    Box::into_raw(Box::new(ch_lance_cancel_handle {
        inner: Arc::new(ScanCancel::new()),
    }))
}

unsafe fn ch_lance_cancel_handle_cancel_impl(handle: *mut ch_lance_cancel_handle) {
    if handle.is_null() {
        return;
    }
    (*handle).inner.cancel();
}

unsafe fn ch_lance_cancel_handle_free_impl(handle: *mut ch_lance_cancel_handle) {
    if !handle.is_null() {
        drop(Box::from_raw(handle));
    }
}

unsafe fn ch_lance_free_dataset_impl(dataset: *mut ch_lance_dataset) {
    if !dataset.is_null() {
        drop(Box::from_raw(dataset));
    }
}

unsafe fn ch_lance_current_snapshot_impl(
    dataset: *mut ch_lance_dataset,
    snapshot: *mut ch_lance_snapshot_info,
    error: *mut ch_lance_error,
) -> bool {
    clear_error(error);
    if dataset.is_null() || snapshot.is_null() {
        set_error(
            error,
            FfiError::invalid_argument("Lance dataset or snapshot pointer is null"),
        );
        return false;
    }

    let dataset = &*dataset;
    match block_on_lance(snapshot_identity(&dataset.dataset, dataset.origin)) {
        Ok(Ok(identity)) => {
            *snapshot = identity;
            true
        }
        Ok(Err(ffi_error)) | Err(ffi_error) => {
            set_error(error, ffi_error);
            false
        }
    }
}

unsafe fn ch_lance_export_schema_impl(
    dataset: *mut ch_lance_dataset,
    snapshot: *const ch_lance_snapshot_info,
    schema: *mut FFI_ArrowSchema,
    cancel: *mut ch_lance_cancel_handle,
    error: *mut ch_lance_error,
) -> bool {
    clear_error(error);
    if dataset.is_null() {
        set_error(
            error,
            FfiError::invalid_argument("Lance dataset pointer is null"),
        );
        return false;
    }

    let dataset_handle = &*dataset;
    let origin = dataset_handle.origin;
    let snapshot = match snapshot_from_ffi(snapshot, origin) {
        Ok(snapshot) => snapshot,
        Err(ffi_error) => {
            set_error(error, ffi_error);
            return false;
        }
    };
    let source = dataset_handle.dataset.clone();
    let cancel = optional_cancel_from_ptr(cancel);
    let dataset = match block_on_lance(with_cancel(cancel.as_deref(), async move {
        checkout_exact_snapshot(&source, snapshot, origin).await
    })) {
        Ok(Ok(dataset)) => dataset,
        Ok(Err(ffi_error)) | Err(ffi_error) => {
            set_error(error, ffi_error);
            return false;
        }
    };
    let arrow_schema = dataset.schema().into();
    match validate_schema(&arrow_schema, origin)
        .and_then(|()| write_schema(arrow_schema, schema, origin))
    {
        Ok(()) => true,
        Err(ffi_error) => {
            set_error(error, ffi_error);
            false
        }
    }
}

unsafe fn ch_lance_total_rows_impl(
    dataset: *mut ch_lance_dataset,
    snapshot: *const ch_lance_snapshot_info,
    rows: *mut u64,
    has_value: *mut bool,
    cancel: *mut ch_lance_cancel_handle,
    error: *mut ch_lance_error,
) -> bool {
    clear_error(error);
    if dataset.is_null() || rows.is_null() || has_value.is_null() {
        set_error(
            error,
            FfiError::invalid_argument("Lance dataset, rows, or has_value pointer is null"),
        );
        return false;
    }
    *has_value = false;

    let dataset = &mut *dataset;
    let origin = dataset.origin;
    let snapshot = match snapshot_from_ffi(snapshot, origin) {
        Ok(snapshot) => snapshot,
        Err(ffi_error) => {
            set_error(error, ffi_error);
            return false;
        }
    };
    let source = dataset.dataset.clone();
    let cancel = optional_cancel_from_ptr(cancel);
    let count_result = block_on_lance(with_cancel(cancel.as_deref(), async move {
        let dataset = checkout_exact_snapshot(&source, snapshot, origin).await?;
        dataset
            .count_rows(None)
            .await
            .map_err(|err| FfiError::from_lance(LanceOperation::CountRows, origin, err))
    }));

    match count_result {
        Ok(Ok(count)) => {
            *rows = count as u64;
            *has_value = true;
            true
        }
        Ok(Err(ffi_error)) | Err(ffi_error) => {
            set_error(error, ffi_error);
            false
        }
    }
}

unsafe fn ch_lance_count_rows_impl(
    dataset: *mut ch_lance_dataset,
    snapshot: *const ch_lance_snapshot_info,
    predicate: *const c_char,
    fragment_ids: *const u64,
    fragment_ids_size: usize,
    rows: *mut u64,
    has_value: *mut bool,
    cancel: *mut ch_lance_cancel_handle,
    error: *mut ch_lance_error,
) -> bool {
    clear_error(error);
    if dataset.is_null() || rows.is_null() || has_value.is_null() {
        set_error(
            error,
            FfiError::invalid_argument("Lance dataset, rows, or has_value pointer is null"),
        );
        return false;
    }
    *has_value = false;

    let predicate = match cstr_to_string(predicate) {
        Ok(predicate) => predicate,
        Err(ffi_error) => {
            set_error(error, ffi_error);
            return false;
        }
    };
    let fragment_ids = match fragment_ids_from_ffi(fragment_ids, fragment_ids_size) {
        Ok(fragment_ids) => fragment_ids,
        Err(ffi_error) => {
            set_error(error, ffi_error);
            return false;
        }
    };

    let dataset = &mut *dataset;
    let origin = dataset.origin;
    let snapshot = match snapshot_from_ffi(snapshot, origin) {
        Ok(snapshot) => snapshot,
        Err(ffi_error) => {
            set_error(error, ffi_error);
            return false;
        }
    };
    let source = dataset.dataset.clone();
    let cancel = optional_cancel_from_ptr(cancel);
    let count_result = block_on_lance(with_cancel(cancel.as_deref(), async move {
        let dataset = checkout_exact_snapshot(&source, snapshot, origin).await?;
        let mut scanner = dataset.scan();
        if !predicate.is_empty() {
            scanner
                .filter(&predicate)
                .map_err(|err| FfiError::from_lance(LanceOperation::CountRows, origin, err))?;
        }
        if let Some(ids) = fragment_ids {
            let all = dataset
                .fragments()
                .iter()
                .map(|fragment| (fragment.id, fragment))
                .collect::<HashMap<_, _>>();
            let mut selected = Vec::with_capacity(ids.len());
            for id in &ids {
                match all.get(id) {
                    Some(fragment) => selected.push((**fragment).clone()),
                    None => {
                        return Err(FfiError::invalid_argument(format!(
                            "Lance fragment id {} is not present in dataset version {}",
                            id,
                            dataset.version().version
                        ))
                        .with_origin(origin));
                    }
                }
            }
            scanner.with_fragments(selected);
        }
        scanner
            .empty_project()
            .map_err(|err| FfiError::from_lance(LanceOperation::CountRows, origin, err))?
            .with_row_id();
        scanner
            .count_rows()
            .await
            .map_err(|err| FfiError::from_lance(LanceOperation::CountRows, origin, err))
    }));

    match count_result {
        Ok(Ok(count)) => {
            *rows = count as u64;
            *has_value = true;
            true
        }
        Ok(Err(ffi_error)) | Err(ffi_error) => {
            set_error(error, ffi_error);
            false
        }
    }
}

unsafe fn ch_lance_total_bytes_impl(
    dataset: *mut ch_lance_dataset,
    bytes: *mut u64,
    has_value: *mut bool,
    error: *mut ch_lance_error,
) -> bool {
    clear_error(error);
    if dataset.is_null() || bytes.is_null() || has_value.is_null() {
        set_error(
            error,
            FfiError::invalid_argument("Lance dataset, bytes, or has_value pointer is null"),
        );
        return false;
    }
    // Lance 2.0.1 does not expose a stable current-snapshot physical byte size
    // through this API. Do not guess from storage listings because that would
    // mix versions and hide object-store errors.
    *has_value = false;
    true
}

unsafe fn ch_lance_list_fragments_impl(
    dataset: *mut ch_lance_dataset,
    snapshot: *const ch_lance_snapshot_info,
    out_list: *mut *mut ch_lance_fragment_info,
    out_size: *mut usize,
    cancel: *mut ch_lance_cancel_handle,
    error: *mut ch_lance_error,
) -> bool {
    clear_error(error);
    if !out_list.is_null() {
        *out_list = ptr::null_mut();
    }
    if !out_size.is_null() {
        *out_size = 0;
    }
    if dataset.is_null() || out_list.is_null() || out_size.is_null() {
        set_error(
            error,
            FfiError::invalid_argument("Lance dataset or fragment list output pointer is null"),
        );
        return false;
    }

    let source = (*dataset).dataset.clone();
    let origin = (*dataset).origin;
    let snapshot = match snapshot_from_ffi(snapshot, origin) {
        Ok(snapshot) => snapshot,
        Err(ffi_error) => {
            set_error(error, ffi_error);
            return false;
        }
    };
    let cancel_token = cancel_arc_from_ptr(cancel);

    let list_result = block_on_lance(with_cancel(Some(cancel_token.as_ref()), async move {
        let dataset = checkout_exact_snapshot(&source, snapshot, origin).await?;
        Ok::<_, FfiError>(list_fragment_infos(&dataset))
    }));

    match list_result {
        Ok(Ok(infos)) => {
            if infos.is_empty() {
                *out_list = ptr::null_mut();
                *out_size = 0;
            } else {
                let mut boxed = infos.into_boxed_slice();
                *out_size = boxed.len();
                *out_list = boxed.as_mut_ptr();
                std::mem::forget(boxed);
            }
            true
        }
        Ok(Err(ffi_error)) | Err(ffi_error) => {
            set_error(error, ffi_error);
            false
        }
    }
}

unsafe fn ch_lance_free_fragment_list_impl(list: *mut ch_lance_fragment_info, size: usize) {
    if list.is_null() || size == 0 {
        return;
    }
    drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
        list, size,
    )));
}

unsafe fn ch_lance_plan_scan_impl(
    dataset: *mut ch_lance_dataset,
    options: *const ch_lance_scan_options,
    error: *mut ch_lance_error,
) -> *mut ch_lance_scan {
    clear_error(error);
    if dataset.is_null() || options.is_null() {
        set_error(
            error,
            FfiError::invalid_argument("Lance dataset or scan options pointer is null"),
        );
        return std::ptr::null_mut();
    }

    let projection = match projection_from_ffi(&(*options).projection) {
        Ok(projection) => projection,
        Err(ffi_error) => {
            set_error(error, ffi_error);
            return std::ptr::null_mut();
        }
    };
    let predicate = match cstr_to_string((*options).predicate) {
        Ok(predicate) => predicate,
        Err(ffi_error) => {
            set_error(error, ffi_error);
            return std::ptr::null_mut();
        }
    };
    let snapshot = match snapshot_from_ffi(&(*options).snapshot, (*dataset).origin) {
        Ok(snapshot) => snapshot,
        Err(ffi_error) => {
            set_error(error, ffi_error);
            return std::ptr::null_mut();
        }
    };
    let max_block_size = (*options).max_block_size as usize;
    let row_limit = (*options).limit;
    let scan_unordered = (*options).scan_unordered;
    let fragment_readahead = (*options).fragment_readahead;
    let batch_readahead = (*options).batch_readahead;
    let io_buffer_size = (*options).io_buffer_size;
    let fragment_ids =
        match fragment_ids_from_ffi((*options).fragment_ids, (*options).fragment_ids_size) {
            Ok(fragment_ids) => fragment_ids,
            Err(ffi_error) => {
                set_error(error, ffi_error);
                return std::ptr::null_mut();
            }
        };
    let source_dataset = (*dataset).dataset.clone();
    let origin = (*dataset).origin;
    let cancel = cancel_arc_from_ptr((*options).cancel);

    LANCE_PLAN_SCAN_CALLS.fetch_add(1, Ordering::Relaxed);
    let cancel_for_plan = Arc::clone(&cancel);
    let stream_result = block_on_lance(with_cancel(Some(cancel_for_plan.as_ref()), async move {
        let dataset = checkout_exact_snapshot(&source_dataset, snapshot, origin).await?;

        let mut scanner = dataset.scan();
        if !projection.is_empty() {
            scanner
                .project(&projection)
                .map_err(|err| FfiError::from_lance(LanceOperation::PlanScan, origin, err))?;
        }
        if !predicate.is_empty() {
            scanner
                .filter(&predicate)
                .map_err(|err| FfiError::from_lance(LanceOperation::PlanScan, origin, err))?;
        }
        if max_block_size != 0 {
            scanner.batch_size(max_block_size);
        }
        if row_limit != 0 {
            // i64::MAX is far beyond practical row caps; saturate rather than fail open.
            let limit_i64 = i64::try_from(row_limit).unwrap_or(i64::MAX);
            scanner
                .limit(Some(limit_i64), None)
                .map_err(|err| FfiError::from_lance(LanceOperation::PlanScan, origin, err))?;
        }
        // false (zero-init) keeps ordered scan (SDK default / ClickHouse compatible).
        scanner.scan_in_order(!scan_unordered);
        if fragment_readahead > 0 {
            scanner.fragment_readahead(fragment_readahead as usize);
        }
        if batch_readahead > 0 {
            scanner.batch_readahead(batch_readahead as usize);
        }
        if io_buffer_size > 0 {
            scanner.io_buffer_size(io_buffer_size);
        }
        if let Some(ids) = fragment_ids {
            let all = dataset
                .fragments()
                .iter()
                .map(|fragment| (fragment.id, fragment))
                .collect::<HashMap<_, _>>();
            let mut selected = Vec::with_capacity(ids.len());
            for id in &ids {
                match all.get(id) {
                    Some(fragment) => selected.push((**fragment).clone()),
                    None => {
                        return Err(FfiError::invalid_argument(format!(
                            "Lance fragment id {} is not present in dataset version {}",
                            id,
                            dataset.version().version
                        ))
                        .with_origin(origin));
                    }
                }
            }
            scanner.with_fragments(selected);
        }
        scanner
            .try_into_stream()
            .await
            .map_err(|err| FfiError::from_lance(LanceOperation::PlanScan, origin, err))
    }));

    match stream_result {
        Ok(Ok(stream)) => Box::into_raw(Box::new(ch_lance_scan {
            stream: Mutex::new(Some(Box::pin(stream))),
            origin,
            cancel,
        })),
        Ok(Err(ffi_error)) | Err(ffi_error) => {
            set_error(error, ffi_error);
            std::ptr::null_mut()
        }
    }
}

unsafe fn ch_lance_next_batch_impl(
    scan: *mut ch_lance_scan,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    has_batch: *mut bool,
    error: *mut ch_lance_error,
) -> bool {
    clear_error(error);
    if scan.is_null() || has_batch.is_null() {
        set_error(
            error,
            FfiError::invalid_argument("Lance scan or has_batch pointer is null"),
        );
        return false;
    }
    *has_batch = false;

    // Shared borrow only: concurrent ch_lance_cancel_scan only touches Arc cancel state.
    let scan = &*scan;
    LANCE_NEXT_BATCH_CALLS.fetch_add(1, Ordering::Relaxed);

    let mut stream_guard = match scan.stream.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };

    if scan.cancel.is_cancelled() {
        // Drop stream so Lance ScanScheduler cancels queued I/O.
        let _ = stream_guard.take();
        set_error(error, FfiError::cancelled());
        return false;
    }

    if stream_guard.is_none() {
        // Already cancelled / EOF'd: surface cancel if requested, else EOF.
        if scan.cancel.is_cancelled() {
            set_error(error, FfiError::cancelled());
            return false;
        }
        return true;
    }

    let cancel = Arc::clone(&scan.cancel);
    let stream = stream_guard.as_mut().expect("stream checked above");
    // Hold the mutex across block_on: only one next_batch at a time (pipeline serial),
    // and cancel_scan never acquires this mutex.
    let next = match block_on_lance(next_batch_or_cancel(stream, cancel.as_ref())) {
        Ok(result) => result,
        Err(ffi_error) => {
            set_error(error, ffi_error);
            return false;
        }
    };

    let next = match next {
        Ok(item) => item,
        Err(ffi_error) => {
            // Cancelled while waiting: drop stream under the mutex.
            let _ = stream_guard.take();
            set_error(error, ffi_error);
            return false;
        }
    };

    match next {
        None => {
            // EOF: drop stream to release scheduler resources promptly.
            let _ = stream_guard.take();
            true
        }
        Some(Ok(batch)) => {
            // Release the stream mutex before Arrow export (CPU work).
            drop(stream_guard);
            match write_record_batch(batch, array, schema, scan.origin) {
                Ok(()) => {
                    *has_batch = true;
                    true
                }
                Err(ffi_error) => {
                    set_error(error, ffi_error);
                    false
                }
            }
        }
        Some(Err(err)) => {
            set_error(
                error,
                FfiError::from_lance(LanceOperation::NextBatch, scan.origin, err),
            );
            false
        }
    }
}

/// Request cooperative cancellation. Thread-safe w.r.t. ch_lance_next_batch.
/// Does not free the scan or drop the stream (that happens in next_batch / free_scan).
unsafe fn ch_lance_cancel_scan_impl(scan: *mut ch_lance_scan) {
    if scan.is_null() {
        return;
    }
    // Shared borrow: only signals Arc cancel state; does not touch the stream mutex.
    (*scan).cancel.cancel();
}

unsafe fn ch_lance_free_scan_impl(scan: *mut ch_lance_scan) {
    if !scan.is_null() {
        // Dropping the box drops the stream (if still present), which cancels queued I/O.
        // Caller must ensure no concurrent next_batch (ClickHouse Scan lifetime).
        drop(Box::from_raw(scan));
    }
}

unsafe fn ch_lance_free_error_impl(error: *mut ch_lance_error) {
    clear_error(error);
}

const MAX_PANIC_DIAGNOSTIC_BYTES: usize = 512;

fn panic_diagnostic(payload: &(dyn Any + Send)) -> String {
    let detail = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("non-string panic payload");
    let mut end = detail.len().min(MAX_PANIC_DIAGNOSTIC_BYTES);
    while !detail.is_char_boundary(end) {
        end -= 1;
    }
    let detail = detail[..end].replace('\0', "?");
    format!("Rust panic caught at Lance FFI boundary: {}", detail)
}

fn set_panic_error(error: *mut ch_lance_error, payload: &(dyn Any + Send)) {
    let message = panic_diagnostic(payload);
    let _ = catch_unwind(AssertUnwindSafe(|| {
        set_error(
            error,
            FfiError::internal(ChLanceErrorOrigin::Unknown, message),
        );
    }));
}

fn ffi_bool_guard(
    error: *mut ch_lance_error,
    operation: impl FnOnce() -> bool,
    reset_outputs: impl FnOnce(),
) -> bool {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(result) => result,
        Err(payload) => {
            let _ = catch_unwind(AssertUnwindSafe(reset_outputs));
            set_panic_error(error, payload.as_ref());
            false
        }
    }
}

fn ffi_ptr_guard<T>(error: *mut ch_lance_error, operation: impl FnOnce() -> *mut T) -> *mut T {
    match catch_unwind(AssertUnwindSafe(operation)) {
        Ok(result) => result,
        Err(payload) => {
            set_panic_error(error, payload.as_ref());
            ptr::null_mut()
        }
    }
}

fn ffi_void_guard(operation: impl FnOnce()) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(operation)) {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            eprintln!("{}", panic_diagnostic(payload.as_ref()));
        }));
    }
}

#[cfg(test)]
static PANIC_NEXT_FFI_CALL: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
fn maybe_inject_test_panic() {
    if PANIC_NEXT_FFI_CALL.swap(false, Ordering::AcqRel) {
        panic!("injected Lance FFI panic");
    }
}

#[cfg(not(test))]
fn maybe_inject_test_panic() {}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_runtime_ensure(
    config: *const ch_lance_runtime_config,
    error: *mut ch_lance_error,
) -> bool {
    ffi_bool_guard(
        error,
        || {
            maybe_inject_test_panic();
            ch_lance_runtime_ensure_impl(config, error)
        },
        || {},
    )
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_get_runtime_stats(out: *mut ch_lance_runtime_stats) {
    ffi_void_guard(|| {
        if !out.is_null() {
            ptr::write(
                out,
                ch_lance_runtime_stats {
                    open_dataset_calls: 0,
                    plan_scan_calls: 0,
                    next_batch_calls: 0,
                    runtime_initialized: 0,
                },
            );
        }
        ch_lance_get_runtime_stats_impl(out);
    });
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_open_dataset(
    options: *const ch_lance_dataset_options,
    error: *mut ch_lance_error,
) -> *mut ch_lance_dataset {
    ffi_ptr_guard(error, || ch_lance_open_dataset_impl(options, error))
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_cancel_handle_create() -> *mut ch_lance_cancel_handle {
    ffi_ptr_guard(ptr::null_mut(), || {
        maybe_inject_test_panic();
        ch_lance_cancel_handle_create_impl()
    })
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_cancel_handle_cancel(handle: *mut ch_lance_cancel_handle) {
    ffi_void_guard(|| ch_lance_cancel_handle_cancel_impl(handle));
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_cancel_handle_free(handle: *mut ch_lance_cancel_handle) {
    ffi_void_guard(|| ch_lance_cancel_handle_free_impl(handle));
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_free_dataset(dataset: *mut ch_lance_dataset) {
    ffi_void_guard(|| ch_lance_free_dataset_impl(dataset));
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_current_snapshot(
    dataset: *mut ch_lance_dataset,
    snapshot: *mut ch_lance_snapshot_info,
    error: *mut ch_lance_error,
) -> bool {
    let reset = || {
        if !snapshot.is_null() {
            ptr::write(snapshot, ch_lance_snapshot_info::default());
        }
    };
    reset();
    ffi_bool_guard(
        error,
        || ch_lance_current_snapshot_impl(dataset, snapshot, error),
        reset,
    )
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_export_schema(
    dataset: *mut ch_lance_dataset,
    snapshot: *const ch_lance_snapshot_info,
    schema: *mut FFI_ArrowSchema,
    cancel: *mut ch_lance_cancel_handle,
    error: *mut ch_lance_error,
) -> bool {
    let reset = || {
        if !schema.is_null() {
            ptr::write(schema, FFI_ArrowSchema::empty());
        }
    };
    reset();
    ffi_bool_guard(
        error,
        || ch_lance_export_schema_impl(dataset, snapshot, schema, cancel, error),
        reset,
    )
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_total_rows(
    dataset: *mut ch_lance_dataset,
    snapshot: *const ch_lance_snapshot_info,
    rows: *mut u64,
    has_value: *mut bool,
    cancel: *mut ch_lance_cancel_handle,
    error: *mut ch_lance_error,
) -> bool {
    let reset = || {
        if !rows.is_null() {
            *rows = 0;
        }
        if !has_value.is_null() {
            *has_value = false;
        }
    };
    reset();
    ffi_bool_guard(
        error,
        || ch_lance_total_rows_impl(dataset, snapshot, rows, has_value, cancel, error),
        reset,
    )
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_count_rows(
    dataset: *mut ch_lance_dataset,
    snapshot: *const ch_lance_snapshot_info,
    predicate: *const c_char,
    fragment_ids: *const u64,
    fragment_ids_size: usize,
    rows: *mut u64,
    has_value: *mut bool,
    cancel: *mut ch_lance_cancel_handle,
    error: *mut ch_lance_error,
) -> bool {
    let reset = || {
        if !rows.is_null() {
            *rows = 0;
        }
        if !has_value.is_null() {
            *has_value = false;
        }
    };
    reset();
    ffi_bool_guard(
        error,
        || {
            ch_lance_count_rows_impl(
                dataset,
                snapshot,
                predicate,
                fragment_ids,
                fragment_ids_size,
                rows,
                has_value,
                cancel,
                error,
            )
        },
        reset,
    )
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_total_bytes(
    dataset: *mut ch_lance_dataset,
    bytes: *mut u64,
    has_value: *mut bool,
    error: *mut ch_lance_error,
) -> bool {
    let reset = || {
        if !bytes.is_null() {
            *bytes = 0;
        }
        if !has_value.is_null() {
            *has_value = false;
        }
    };
    reset();
    ffi_bool_guard(
        error,
        || ch_lance_total_bytes_impl(dataset, bytes, has_value, error),
        reset,
    )
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_list_fragments(
    dataset: *mut ch_lance_dataset,
    snapshot: *const ch_lance_snapshot_info,
    out_list: *mut *mut ch_lance_fragment_info,
    out_size: *mut usize,
    cancel: *mut ch_lance_cancel_handle,
    error: *mut ch_lance_error,
) -> bool {
    let reset = || {
        if !out_list.is_null() {
            *out_list = ptr::null_mut();
        }
        if !out_size.is_null() {
            *out_size = 0;
        }
    };
    reset();
    ffi_bool_guard(
        error,
        || ch_lance_list_fragments_impl(dataset, snapshot, out_list, out_size, cancel, error),
        reset,
    )
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_free_fragment_list(
    list: *mut ch_lance_fragment_info,
    size: usize,
) {
    ffi_void_guard(|| ch_lance_free_fragment_list_impl(list, size));
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_plan_scan(
    dataset: *mut ch_lance_dataset,
    options: *const ch_lance_scan_options,
    error: *mut ch_lance_error,
) -> *mut ch_lance_scan {
    ffi_ptr_guard(error, || ch_lance_plan_scan_impl(dataset, options, error))
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_next_batch(
    scan: *mut ch_lance_scan,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    has_batch: *mut bool,
    error: *mut ch_lance_error,
) -> bool {
    let reset = || {
        if !array.is_null() {
            ptr::write(array, FFI_ArrowArray::empty());
        }
        if !schema.is_null() {
            ptr::write(schema, FFI_ArrowSchema::empty());
        }
        if !has_batch.is_null() {
            *has_batch = false;
        }
    };
    reset();
    ffi_bool_guard(
        error,
        || ch_lance_next_batch_impl(scan, array, schema, has_batch, error),
        reset,
    )
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_cancel_scan(scan: *mut ch_lance_scan) {
    ffi_void_guard(|| ch_lance_cancel_scan_impl(scan));
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_free_scan(scan: *mut ch_lance_scan) {
    ffi_void_guard(|| ch_lance_free_scan_impl(scan));
}

#[no_mangle]
pub unsafe extern "C" fn ch_lance_free_error(error: *mut ch_lance_error) {
    ffi_void_guard(|| {
        maybe_inject_test_panic();
        ch_lance_free_error_impl(error);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ffi::from_ffi;
    use arrow_array::ffi::FFI_ArrowArray;
    use arrow_array::{
        make_array, Date32Array, Float64Array, Int32Array, RecordBatch, RecordBatchIterator,
        StringArray, TimestampMillisecondArray,
    };
    use arrow_schema::{DataType, Field, Schema};
    use std::ptr::addr_of_mut;
    use std::sync::Arc;

    fn dataset_options(uri: &CString) -> ch_lance_dataset_options {
        ch_lance_dataset_options {
            uri: uri.as_ptr(),
            use_s3: false,
            s3_region: ptr::null(),
            s3_endpoint: ptr::null(),
            s3_access_key_id: ptr::null(),
            s3_secret_access_key: ptr::null(),
            s3_session_token: ptr::null(),
            s3_role_arn: ptr::null(),
            s3_role_session_name: ptr::null(),
            s3_use_environment_credentials: false,
            s3_no_sign_request: false,
            s3_allow_http: false,
            s3_virtual_hosted_style_request: false,
            s3_request_timeout_ms: 0,
            s3_connect_timeout_ms: 0,
            cancel: ptr::null_mut(),
        }
    }

    fn empty_error() -> ch_lance_error {
        ch_lance_error {
            kind: ChLanceErrorKind::None as u32,
            origin: ChLanceErrorOrigin::Unknown as u32,
            message: ptr::null_mut(),
        }
    }

    fn error_message(error: &ch_lance_error) -> String {
        if error.message.is_null() {
            return format!("FFI error kind {} without a message", error.kind);
        }
        unsafe { CStr::from_ptr(error.message) }
            .to_string_lossy()
            .into_owned()
    }

    fn make_batch(values: Vec<i32>) -> RecordBatch {
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int32Array::from(values)) as arrow_array::ArrayRef,
        )])
        .unwrap();
        batch
    }

    fn write_test_dataset() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_batch(vec![1, 2, 3]);
        let schema = batch.schema();
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        Runtime::new()
            .unwrap()
            .block_on(Dataset::write(reader, dir.path().to_str().unwrap(), None))
            .unwrap();
        dir
    }

    fn write_empty_dataset() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_batch(Vec::new());
        let schema = batch.schema();
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        Runtime::new()
            .unwrap()
            .block_on(Dataset::write(reader, dir.path().to_str().unwrap(), None))
            .unwrap();
        dir
    }

    fn make_pushdown_batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as arrow_array::ArrayRef,
            ),
            (
                "name",
                Arc::new(StringArray::from(vec!["a", "quote'd", "x", "b", "x"]))
                    as arrow_array::ArrayRef,
            ),
            (
                "score",
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    None,
                    Some(3.0),
                    Some(4.0),
                    None,
                ])) as arrow_array::ArrayRef,
            ),
            (
                "event_date",
                Arc::new(Date32Array::from(vec![19723, 19724, 19725, 19726, 19727]))
                    as arrow_array::ArrayRef,
            ),
            (
                "event_time",
                Arc::new(TimestampMillisecondArray::from(vec![
                    1_704_067_200_000,
                    1_704_164_645_123,
                    1_704_240_000_000,
                    1_704_326_400_000,
                    1_704_412_800_000,
                ])) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn write_pushdown_dataset() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let batch = make_pushdown_batch();
        let schema = batch.schema();
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        Runtime::new()
            .unwrap()
            .block_on(Dataset::write(reader, dir.path().to_str().unwrap(), None))
            .unwrap();
        dir
    }

    fn scan_row_count(
        dataset: *mut ch_lance_dataset,
        snapshot: ch_lance_snapshot_info,
        projection: &[CString],
        predicate: &CString,
        error: *mut ch_lance_error,
    ) -> usize {
        let projection_ptrs = projection
            .iter()
            .map(|value| value.as_ptr())
            .collect::<Vec<_>>();
        let scan_options = ch_lance_scan_options {
            snapshot,
            projection: ch_lance_string_list {
                values: projection_ptrs.as_ptr(),
                size: projection_ptrs.len(),
            },
            predicate: predicate.as_ptr(),
            need_only_count: false,
            max_block_size: 1024,
            limit: 0,
            cancel: ptr::null_mut(),
            scan_unordered: false,
            fragment_readahead: 0,
            batch_readahead: 0,
            io_buffer_size: 0,
            fragment_ids: ptr::null(),
            fragment_ids_size: 0,
        };
        let scan = unsafe { ch_lance_plan_scan(dataset, &scan_options, error) };
        assert!(!scan.is_null());

        let mut rows = 0;
        loop {
            let mut array = FFI_ArrowArray::empty();
            let mut schema = FFI_ArrowSchema::empty();
            let mut has_batch = false;
            assert!(unsafe {
                ch_lance_next_batch(
                    scan,
                    addr_of_mut!(array),
                    addr_of_mut!(schema),
                    addr_of_mut!(has_batch),
                    error,
                )
            });
            if !has_batch {
                break;
            }
            let struct_data = unsafe { from_ffi(array, &schema) }.unwrap();
            rows += StructArray::from(make_array(struct_data).to_data()).len();
        }
        unsafe {
            ch_lance_free_scan(scan);
        }
        rows
    }

    fn scan_rows_with_options(
        dataset: *mut ch_lance_dataset,
        snapshot: ch_lance_snapshot_info,
        scan_unordered: bool,
        fragment_readahead: u32,
        batch_readahead: u32,
        fragment_ids: Option<&[u64]>,
        error: *mut ch_lance_error,
    ) -> usize {
        let column = CString::new("id").unwrap();
        let projection_values = [column.as_ptr()];
        let scan_options = ch_lance_scan_options {
            snapshot,
            projection: ch_lance_string_list {
                values: projection_values.as_ptr(),
                size: projection_values.len(),
            },
            predicate: ptr::null(),
            need_only_count: false,
            max_block_size: 1024,
            limit: 0,
            cancel: ptr::null_mut(),
            scan_unordered,
            fragment_readahead,
            batch_readahead,
            io_buffer_size: 0,
            fragment_ids: fragment_ids.map(|ids| ids.as_ptr()).unwrap_or(ptr::null()),
            fragment_ids_size: fragment_ids.map(|ids| ids.len()).unwrap_or(0),
        };
        let scan = unsafe { ch_lance_plan_scan(dataset, &scan_options, error) };
        assert!(!scan.is_null(), "plan_scan failed");
        let mut rows = 0;
        loop {
            let mut array = FFI_ArrowArray::empty();
            let mut schema = FFI_ArrowSchema::empty();
            let mut has_batch = false;
            assert!(unsafe {
                ch_lance_next_batch(
                    scan,
                    addr_of_mut!(array),
                    addr_of_mut!(schema),
                    addr_of_mut!(has_batch),
                    error,
                )
            });
            if !has_batch {
                break;
            }
            let struct_data = unsafe { from_ffi(array, &schema) }.unwrap();
            rows += StructArray::from(make_array(struct_data).to_data()).len();
        }
        unsafe {
            ch_lance_free_scan(scan);
        }
        rows
    }

    #[test]
    fn ffi_unordered_scan_with_fragment_readahead_preserves_row_count() {
        let dir = write_test_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = dataset_options(&uri);
        let mut error = empty_error();

        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());

        let mut snapshot = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(snapshot), addr_of_mut!(error))
        });

        let ordered_rows =
            scan_rows_with_options(dataset, snapshot, false, 0, 0, None, addr_of_mut!(error));
        let unordered_rows =
            scan_rows_with_options(dataset, snapshot, true, 4, 2, None, addr_of_mut!(error));
        assert_eq!(ordered_rows, 3);
        assert_eq!(unordered_rows, ordered_rows);

        unsafe {
            ch_lance_free_dataset(dataset);
        }
    }

    fn write_multi_fragment_dataset() -> tempfile::TempDir {
        use lance::dataset::WriteParams;
        let dir = tempfile::tempdir().unwrap();
        // 6 rows with max_rows_per_file=2 → expect 3 fragments.
        let batch = make_batch(vec![1, 2, 3, 4, 5, 6]);
        let schema = batch.schema();
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        let params = WriteParams {
            max_rows_per_file: 2,
            ..Default::default()
        };
        Runtime::new()
            .unwrap()
            .block_on(Dataset::write(
                reader,
                dir.path().to_str().unwrap(),
                Some(params),
            ))
            .unwrap();
        dir
    }

    #[test]
    fn ffi_list_fragments_and_subset_scan() {
        let dir = write_multi_fragment_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = dataset_options(&uri);
        let mut error = empty_error();

        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());

        let mut snapshot = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(snapshot), addr_of_mut!(error))
        });

        let mut list: *mut ch_lance_fragment_info = ptr::null_mut();
        let mut size: usize = 0;
        assert!(unsafe {
            ch_lance_list_fragments(
                dataset,
                &snapshot,
                addr_of_mut!(list),
                addr_of_mut!(size),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });
        assert!(size >= 2, "expected multi-fragment dataset, got {}", size);
        assert!(!list.is_null());

        let infos = unsafe { std::slice::from_raw_parts(list, size) };
        let total_rows: u64 = infos
            .iter()
            .map(|info| {
                assert_ne!(info.num_rows, u64::MAX);
                info.num_rows
            })
            .sum();
        assert_eq!(total_rows, 6);

        let all_ids: Vec<u64> = infos.iter().map(|info| info.id).collect();
        let full_rows =
            scan_rows_with_options(dataset, snapshot, false, 0, 0, None, addr_of_mut!(error));
        assert_eq!(full_rows, 6);

        let first = [all_ids[0]];
        let first_rows = scan_rows_with_options(
            dataset,
            snapshot,
            false,
            0,
            0,
            Some(&first),
            addr_of_mut!(error),
        );
        assert!(first_rows > 0 && first_rows < full_rows);

        let rest = &all_ids[1..];
        let rest_rows = scan_rows_with_options(
            dataset,
            snapshot,
            false,
            0,
            0,
            Some(rest),
            addr_of_mut!(error),
        );
        assert_eq!(first_rows + rest_rows, full_rows);

        let mut first_count = 0;
        let mut has_first_count = false;
        let count_ok = unsafe {
            ch_lance_count_rows(
                dataset,
                &snapshot,
                ptr::null(),
                first.as_ptr(),
                first.len(),
                addr_of_mut!(first_count),
                addr_of_mut!(has_first_count),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        };
        assert!(count_ok, "{}", error_message(&error));
        assert!(has_first_count);
        assert_eq!(first_count as usize, first_rows);

        let predicate = CString::new("id <= 4").unwrap();
        let mut rest_count = 0;
        let mut has_rest_count = false;
        let count_ok = unsafe {
            ch_lance_count_rows(
                dataset,
                &snapshot,
                predicate.as_ptr(),
                rest.as_ptr(),
                rest.len(),
                addr_of_mut!(rest_count),
                addr_of_mut!(has_rest_count),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        };
        assert!(count_ok, "{}", error_message(&error));
        assert!(has_rest_count);
        assert_eq!(rest_count, 2);

        // Missing fragment id → InvalidArgument.
        let missing = [u64::MAX];
        let column = CString::new("id").unwrap();
        let projection_values = [column.as_ptr()];
        let bad_options = ch_lance_scan_options {
            snapshot,
            projection: ch_lance_string_list {
                values: projection_values.as_ptr(),
                size: projection_values.len(),
            },
            predicate: ptr::null(),
            need_only_count: false,
            max_block_size: 1024,
            limit: 0,
            cancel: ptr::null_mut(),
            scan_unordered: false,
            fragment_readahead: 0,
            batch_readahead: 0,
            io_buffer_size: 0,
            fragment_ids: missing.as_ptr(),
            fragment_ids_size: missing.len(),
        };
        let bad_scan = unsafe { ch_lance_plan_scan(dataset, &bad_options, addr_of_mut!(error)) };
        assert!(bad_scan.is_null());
        assert_eq!(error.kind, ChLanceErrorKind::InvalidArgument as u32);
        unsafe { ch_lance_free_error(addr_of_mut!(error)) };

        // Bad version → VersionNotFound.
        let mut missing_snapshot = snapshot;
        missing_snapshot.version += 1_000_000;
        let mut bad_list: *mut ch_lance_fragment_info = ptr::null_mut();
        let mut bad_size: usize = 0;
        assert!(!unsafe {
            ch_lance_list_fragments(
                dataset,
                &missing_snapshot,
                addr_of_mut!(bad_list),
                addr_of_mut!(bad_size),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });
        assert_eq!(error.kind, ChLanceErrorKind::VersionNotFound as u32);
        unsafe { ch_lance_free_error(addr_of_mut!(error)) };

        // Cancel list.
        let cancel = unsafe { ch_lance_cancel_handle_create() };
        unsafe { ch_lance_cancel_handle_cancel(cancel) };
        assert!(!unsafe {
            ch_lance_list_fragments(
                dataset,
                &snapshot,
                addr_of_mut!(bad_list),
                addr_of_mut!(bad_size),
                cancel,
                addr_of_mut!(error),
            )
        });
        assert_eq!(error.kind, ChLanceErrorKind::Cancelled as u32);
        unsafe {
            ch_lance_free_error(addr_of_mut!(error));
            ch_lance_cancel_handle_free(cancel);
            ch_lance_free_fragment_list(list, size);
            ch_lance_free_dataset(dataset);
        }
    }

    #[test]
    fn ffi_open_schema_and_scan_local_dataset() {
        let dir = write_test_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = dataset_options(&uri);
        let mut error = empty_error();

        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());

        let mut snapshot = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(snapshot), addr_of_mut!(error))
        });
        assert!(snapshot.version > 0);

        let mut schema = FFI_ArrowSchema::empty();
        assert!(unsafe {
            ch_lance_export_schema(
                dataset,
                &snapshot,
                addr_of_mut!(schema),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });

        let cancel = unsafe { ch_lance_cancel_handle_create() };
        unsafe { ch_lance_cancel_handle_cancel(cancel) };
        let mut cancelled_schema = FFI_ArrowSchema::empty();
        assert!(!unsafe {
            ch_lance_export_schema(
                dataset,
                &snapshot,
                addr_of_mut!(cancelled_schema),
                cancel,
                addr_of_mut!(error),
            )
        });
        assert_eq!(error.kind, ChLanceErrorKind::Cancelled as u32);
        unsafe {
            ch_lance_free_error(addr_of_mut!(error));
            ch_lance_cancel_handle_free(cancel);
        }

        let column = CString::new("id").unwrap();
        let projection_values = [column.as_ptr()];
        let predicate = CString::new("id = 2").unwrap();
        let scan_options = ch_lance_scan_options {
            snapshot,
            projection: ch_lance_string_list {
                values: projection_values.as_ptr(),
                size: projection_values.len(),
            },
            predicate: predicate.as_ptr(),
            need_only_count: false,
            max_block_size: 1024,
            limit: 0,
            cancel: ptr::null_mut(),
            scan_unordered: false,
            fragment_readahead: 0,
            batch_readahead: 0,
            io_buffer_size: 0,
            fragment_ids: ptr::null(),
            fragment_ids_size: 0,
        };
        let scan = unsafe { ch_lance_plan_scan(dataset, &scan_options, addr_of_mut!(error)) };
        assert!(!scan.is_null());

        let mut array = FFI_ArrowArray::empty();
        let mut batch_schema = FFI_ArrowSchema::empty();
        let mut has_batch = false;
        assert!(unsafe {
            ch_lance_next_batch(
                scan,
                addr_of_mut!(array),
                addr_of_mut!(batch_schema),
                addr_of_mut!(has_batch),
                addr_of_mut!(error),
            )
        });
        assert!(has_batch);

        let struct_data = unsafe { from_ffi(array, &batch_schema) }.unwrap();
        let struct_array = StructArray::from(make_array(struct_data).to_data());
        assert_eq!(struct_array.len(), 1);

        unsafe {
            ch_lance_free_scan(scan);
            ch_lance_free_dataset(dataset);
        }
    }

    fn plan_full_scan(
        dataset: *mut ch_lance_dataset,
        snapshot: ch_lance_snapshot_info,
        error: *mut ch_lance_error,
    ) -> *mut ch_lance_scan {
        let column = CString::new("id").unwrap();
        let projection_values = [column.as_ptr()];
        let scan_options = ch_lance_scan_options {
            snapshot,
            projection: ch_lance_string_list {
                values: projection_values.as_ptr(),
                size: projection_values.len(),
            },
            predicate: ptr::null(),
            need_only_count: false,
            max_block_size: 1,
            limit: 0,
            cancel: ptr::null_mut(),
            scan_unordered: false,
            fragment_readahead: 0,
            batch_readahead: 0,
            io_buffer_size: 0,
            fragment_ids: ptr::null(),
            fragment_ids_size: 0,
        };
        let scan = unsafe { ch_lance_plan_scan(dataset, &scan_options, error) };
        assert!(!scan.is_null());
        scan
    }

    #[test]
    fn ffi_cancel_before_next_returns_cancelled() {
        let dir = write_test_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = dataset_options(&uri);
        let mut error = empty_error();

        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());
        let mut snapshot = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(snapshot), addr_of_mut!(error))
        });

        let scan = plan_full_scan(dataset, snapshot, addr_of_mut!(error));
        unsafe { ch_lance_cancel_scan(scan) };

        let mut array = FFI_ArrowArray::empty();
        let mut batch_schema = FFI_ArrowSchema::empty();
        let mut has_batch = false;
        let ok = unsafe {
            ch_lance_next_batch(
                scan,
                addr_of_mut!(array),
                addr_of_mut!(batch_schema),
                addr_of_mut!(has_batch),
                addr_of_mut!(error),
            )
        };
        assert!(!ok);
        assert!(!has_batch);
        assert_eq!(error.kind, ChLanceErrorKind::Cancelled as u32);
        unsafe {
            ch_lance_free_error(addr_of_mut!(error));
            ch_lance_free_scan(scan);
            ch_lance_free_dataset(dataset);
        }
    }

    #[test]
    fn ffi_cancel_after_partial_scan_returns_cancelled() {
        let dir = write_test_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = dataset_options(&uri);
        let mut error = empty_error();

        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());
        let mut snapshot = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(snapshot), addr_of_mut!(error))
        });

        let scan = plan_full_scan(dataset, snapshot, addr_of_mut!(error));

        let mut array = FFI_ArrowArray::empty();
        let mut batch_schema = FFI_ArrowSchema::empty();
        let mut has_batch = false;
        assert!(unsafe {
            ch_lance_next_batch(
                scan,
                addr_of_mut!(array),
                addr_of_mut!(batch_schema),
                addr_of_mut!(has_batch),
                addr_of_mut!(error),
            )
        });
        assert!(has_batch);
        // Release the imported batch so free is clean if any.
        drop(unsafe { from_ffi(array, &batch_schema) }.unwrap());

        unsafe { ch_lance_cancel_scan(scan) };

        let mut array2 = FFI_ArrowArray::empty();
        let mut batch_schema2 = FFI_ArrowSchema::empty();
        let mut has_batch2 = false;
        let ok = unsafe {
            ch_lance_next_batch(
                scan,
                addr_of_mut!(array2),
                addr_of_mut!(batch_schema2),
                addr_of_mut!(has_batch2),
                addr_of_mut!(error),
            )
        };
        assert!(!ok);
        assert!(!has_batch2);
        assert_eq!(error.kind, ChLanceErrorKind::Cancelled as u32);

        // Double cancel + free must be safe.
        unsafe {
            ch_lance_cancel_scan(scan);
            ch_lance_free_error(addr_of_mut!(error));
            ch_lance_free_scan(scan);
            ch_lance_free_dataset(dataset);
        }
    }

    #[test]
    fn ffi_cancel_wakes_pending_next_batch() {
        use std::thread;
        use std::time::{Duration, Instant};

        // Synthetic stream that stays pending until dropped / cancelled via select.
        let cancel = Arc::new(ScanCancel::new());
        let cancel_for_thread = Arc::clone(&cancel);
        let started = Arc::new(AtomicBool::new(false));
        let started_flag = Arc::clone(&started);

        let join = thread::spawn(move || {
            let runtime = ensure_lance_runtime().expect("runtime");
            runtime.block_on(async move {
                let mut stream: LanceBatchStream = Box::pin(futures::stream::pending());
                started_flag.store(true, Ordering::Release);
                next_batch_or_cancel(&mut stream, cancel_for_thread.as_ref()).await
            })
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        while !started.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "worker did not start");
            thread::sleep(Duration::from_millis(1));
        }
        // Give the worker time to enter select!.
        thread::sleep(Duration::from_millis(20));
        let t0 = Instant::now();
        cancel.cancel();
        let result = join.join().expect("worker thread");
        assert!(
            t0.elapsed() < Duration::from_secs(2),
            "cancel did not wake promptly"
        );
        let err = result.expect_err("expected cancel error");
        assert_eq!(err.kind, ChLanceErrorKind::Cancelled);
    }

    #[test]
    fn ffi_cancel_handle_cancels_plan_and_next() {
        let dir = write_test_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let mut options = dataset_options(&uri);
        let mut error = empty_error();

        let cancel = unsafe { ch_lance_cancel_handle_create() };
        assert!(!cancel.is_null());
        options.cancel = cancel;

        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());

        let mut snapshot = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(snapshot), addr_of_mut!(error))
        });

        // Cancel before plan: planScan must fail with CANCELLED.
        unsafe { ch_lance_cancel_handle_cancel(cancel) };
        let column = CString::new("id").unwrap();
        let projection_values = [column.as_ptr()];
        let scan_options = ch_lance_scan_options {
            snapshot,
            projection: ch_lance_string_list {
                values: projection_values.as_ptr(),
                size: projection_values.len(),
            },
            predicate: ptr::null(),
            need_only_count: false,
            max_block_size: 1,
            limit: 0,
            cancel,
            scan_unordered: false,
            fragment_readahead: 0,
            batch_readahead: 0,
            io_buffer_size: 0,
            fragment_ids: ptr::null(),
            fragment_ids_size: 0,
        };
        let scan = unsafe { ch_lance_plan_scan(dataset, &scan_options, addr_of_mut!(error)) };
        assert!(scan.is_null());
        assert_eq!(error.kind, ChLanceErrorKind::Cancelled as u32);
        unsafe { ch_lance_free_error(addr_of_mut!(error)) };

        // Fresh handle shared with plan + next.
        let cancel2 = unsafe { ch_lance_cancel_handle_create() };
        let scan_options2 = ch_lance_scan_options {
            snapshot,
            projection: ch_lance_string_list {
                values: projection_values.as_ptr(),
                size: projection_values.len(),
            },
            predicate: ptr::null(),
            need_only_count: false,
            max_block_size: 1,
            limit: 0,
            cancel: cancel2,
            scan_unordered: false,
            fragment_readahead: 0,
            batch_readahead: 0,
            io_buffer_size: 0,
            fragment_ids: ptr::null(),
            fragment_ids_size: 0,
        };
        let scan2 = unsafe { ch_lance_plan_scan(dataset, &scan_options2, addr_of_mut!(error)) };
        assert!(!scan2.is_null());

        unsafe { ch_lance_cancel_handle_cancel(cancel2) };
        let mut array = FFI_ArrowArray::empty();
        let mut batch_schema = FFI_ArrowSchema::empty();
        let mut has_batch = false;
        let ok = unsafe {
            ch_lance_next_batch(
                scan2,
                addr_of_mut!(array),
                addr_of_mut!(batch_schema),
                addr_of_mut!(has_batch),
                addr_of_mut!(error),
            )
        };
        assert!(!ok);
        assert_eq!(error.kind, ChLanceErrorKind::Cancelled as u32);

        unsafe {
            ch_lance_free_error(addr_of_mut!(error));
            ch_lance_free_scan(scan2);
            ch_lance_cancel_handle_free(cancel2);
            ch_lance_cancel_handle_free(cancel);
            ch_lance_free_dataset(dataset);
        }
    }

    #[test]
    fn ffi_cancel_handle_cancels_total_rows() {
        let dir = write_test_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = dataset_options(&uri);
        let mut error = empty_error();
        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());
        let mut snapshot = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(snapshot), addr_of_mut!(error))
        });

        let cancel = unsafe { ch_lance_cancel_handle_create() };
        unsafe { ch_lance_cancel_handle_cancel(cancel) };

        let mut rows = 0;
        let mut has_value = true;
        let ok = unsafe {
            ch_lance_total_rows(
                dataset,
                &snapshot,
                addr_of_mut!(rows),
                addr_of_mut!(has_value),
                cancel,
                addr_of_mut!(error),
            )
        };
        assert!(!ok);
        assert!(!has_value);
        assert_eq!(error.kind, ChLanceErrorKind::Cancelled as u32);

        unsafe {
            ch_lance_free_error(addr_of_mut!(error));
            ch_lance_cancel_handle_free(cancel);
            ch_lance_free_dataset(dataset);
        }
    }

    #[test]
    fn parse_timeout_duration_accepts_ms_and_secs() {
        assert_eq!(
            parse_timeout_duration("1500ms").unwrap(),
            Duration::from_millis(1500)
        );
        assert_eq!(
            parse_timeout_duration("30s").unwrap(),
            Duration::from_secs(30)
        );
        assert_eq!(
            parse_timeout_duration("2500").unwrap(),
            Duration::from_millis(2500)
        );
        assert!(parse_timeout_duration("not-a-duration").is_err());
    }

    #[test]
    fn apply_dataset_options_records_timeouts() {
        let uri = CString::new("s3://bucket/path").unwrap();
        let region = CString::new("us-east-1").unwrap();
        let options = ch_lance_dataset_options {
            uri: uri.as_ptr(),
            use_s3: true,
            s3_region: region.as_ptr(),
            s3_endpoint: ptr::null(),
            s3_access_key_id: ptr::null(),
            s3_secret_access_key: ptr::null(),
            s3_session_token: ptr::null(),
            s3_role_arn: ptr::null(),
            s3_role_session_name: ptr::null(),
            s3_use_environment_credentials: false,
            s3_no_sign_request: true,
            s3_allow_http: true,
            s3_virtual_hosted_style_request: false,
            s3_request_timeout_ms: 12_000,
            s3_connect_timeout_ms: 2_000,
            cancel: ptr::null_mut(),
        };
        let open = unsafe { apply_dataset_options(&options) }.unwrap();
        let storage = open.storage_options.expect("s3 options");
        assert_eq!(storage.get("timeout").map(String::as_str), Some("12000ms"));
        assert_eq!(
            storage.get("connect_timeout").map(String::as_str),
            Some("2000ms")
        );
    }

    #[test]
    fn with_cancel_wakes_pending_future() {
        use std::thread;
        use std::time::{Duration, Instant};

        let cancel = Arc::new(ScanCancel::new());
        let cancel_for_thread = Arc::clone(&cancel);
        let started = Arc::new(AtomicBool::new(false));
        let started_flag = Arc::clone(&started);

        let join = thread::spawn(move || {
            let runtime = ensure_lance_runtime().expect("runtime");
            runtime.block_on(async move {
                started_flag.store(true, Ordering::Release);
                with_cancel(Some(cancel_for_thread.as_ref()), async {
                    // Pending forever until cancelled.
                    futures::future::pending::<Result<(), FfiError>>().await
                })
                .await
            })
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        while !started.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "worker did not start");
            thread::sleep(Duration::from_millis(1));
        }
        thread::sleep(Duration::from_millis(20));
        let t0 = Instant::now();
        cancel.cancel();
        let result = join.join().expect("worker thread");
        assert!(t0.elapsed() < Duration::from_secs(2));
        let err = result.expect_err("expected cancel");
        assert_eq!(err.kind, ChLanceErrorKind::Cancelled);
    }

    #[test]
    fn ffi_empty_dataset_is_successful() {
        let dir = write_empty_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = dataset_options(&uri);
        let mut error = empty_error();

        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());
        assert_eq!(error.kind, ChLanceErrorKind::None as u32);

        let mut snapshot = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(snapshot), addr_of_mut!(error))
        });

        let mut rows = 1;
        let mut has_value = false;
        assert!(unsafe {
            ch_lance_total_rows(
                dataset,
                &snapshot,
                addr_of_mut!(rows),
                addr_of_mut!(has_value),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });
        assert!(has_value);
        assert_eq!(rows, 0);
        assert_eq!(error.kind, ChLanceErrorKind::None as u32);

        let projection = [CString::new("id").unwrap()];
        let predicate = CString::new("").unwrap();
        assert_eq!(
            scan_row_count(
                dataset,
                snapshot,
                &projection,
                &predicate,
                addr_of_mut!(error),
            ),
            0
        );
        assert_eq!(error.kind, ChLanceErrorKind::None as u32);

        unsafe {
            ch_lance_free_dataset(dataset);
        }
    }

    #[test]
    fn ffi_distinguishes_missing_dataset_from_invalid_uri() {
        let parent = tempfile::tempdir().unwrap();
        let missing_path = parent.path().join("missing.lance");
        let missing_uri = CString::new(missing_path.to_str().unwrap()).unwrap();
        let missing_options = dataset_options(&missing_uri);
        let mut error = empty_error();

        let dataset = unsafe { ch_lance_open_dataset(&missing_options, addr_of_mut!(error)) };
        assert!(dataset.is_null());
        assert_eq!(error.kind, ChLanceErrorKind::NotFound as u32);
        assert_eq!(error.origin, ChLanceErrorOrigin::Local as u32);
        unsafe {
            ch_lance_free_error(addr_of_mut!(error));
        }

        let empty_uri = CString::new("").unwrap();
        let invalid_options = dataset_options(&empty_uri);
        let dataset = unsafe { ch_lance_open_dataset(&invalid_options, addr_of_mut!(error)) };
        assert!(dataset.is_null());
        assert_eq!(error.kind, ChLanceErrorKind::InvalidArgument as u32);
        assert_eq!(error.origin, ChLanceErrorOrigin::Local as u32);
        unsafe {
            ch_lance_free_error(addr_of_mut!(error));
        }
    }

    #[test]
    fn ffi_total_rows_uses_requested_snapshot() {
        let dir = write_test_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = dataset_options(&uri);
        let mut error = empty_error();

        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());

        let mut snapshot = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(snapshot), addr_of_mut!(error))
        });

        let mut rows = 0;
        let mut has_value = false;
        assert!(unsafe {
            ch_lance_total_rows(
                dataset,
                &snapshot,
                addr_of_mut!(rows),
                addr_of_mut!(has_value),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });
        assert!(has_value);
        assert_eq!(rows, 3);

        let append_batch = make_batch(vec![4]);
        let schema = append_batch.schema();
        let reader = RecordBatchIterator::new(vec![Ok(append_batch)].into_iter(), schema);
        Runtime::new().unwrap().block_on(async {
            let mut dataset = Dataset::open(dir.path().to_str().unwrap()).await.unwrap();
            dataset.append(reader, None).await.unwrap();
        });

        rows = 0;
        has_value = false;
        assert!(unsafe {
            ch_lance_total_rows(
                dataset,
                &snapshot,
                addr_of_mut!(rows),
                addr_of_mut!(has_value),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });
        assert!(has_value);
        assert_eq!(rows, 3);

        let latest_dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!latest_dataset.is_null());
        let mut latest_snapshot = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(
                latest_dataset,
                addr_of_mut!(latest_snapshot),
                addr_of_mut!(error),
            )
        });
        assert!(latest_snapshot.version > snapshot.version);

        rows = 0;
        has_value = false;
        assert!(unsafe {
            ch_lance_total_rows(
                latest_dataset,
                &snapshot,
                addr_of_mut!(rows),
                addr_of_mut!(has_value),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });
        assert!(has_value);
        assert_eq!(rows, 3);

        rows = 0;
        has_value = false;
        assert!(unsafe {
            ch_lance_total_rows(
                latest_dataset,
                &latest_snapshot,
                addr_of_mut!(rows),
                addr_of_mut!(has_value),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });
        assert!(has_value);
        assert_eq!(rows, 4);

        let projection = [CString::new("id").unwrap()];
        let predicate = CString::new("").unwrap();
        assert_eq!(
            scan_row_count(
                latest_dataset,
                snapshot,
                &projection,
                &predicate,
                addr_of_mut!(error),
            ),
            3
        );

        unsafe {
            ch_lance_free_dataset(latest_dataset);
            ch_lance_free_dataset(dataset);
        }
    }

    #[test]
    fn ffi_rejects_same_path_same_version_recreated_dataset() {
        let dir = write_test_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = dataset_options(&uri);
        let mut error = empty_error();

        let original = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!original.is_null());
        let mut pinned = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(original, addr_of_mut!(pinned), addr_of_mut!(error))
        });
        unsafe { ch_lance_free_dataset(original) };

        std::fs::remove_dir_all(dir.path()).unwrap();
        let replacement_batch = make_batch(vec![101, 102]);
        let replacement_schema = replacement_batch.schema();
        let replacement_reader =
            RecordBatchIterator::new(vec![Ok(replacement_batch)].into_iter(), replacement_schema);
        Runtime::new()
            .unwrap()
            .block_on(Dataset::write(
                replacement_reader,
                dir.path().to_str().unwrap(),
                None,
            ))
            .unwrap();

        let replacement = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!replacement.is_null());
        let mut replacement_snapshot = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(
                replacement,
                addr_of_mut!(replacement_snapshot),
                addr_of_mut!(error),
            )
        });
        assert_eq!(replacement_snapshot.version, pinned.version);
        assert_ne!(replacement_snapshot.manifest_sha256, pinned.manifest_sha256);

        let mut schema = FFI_ArrowSchema::empty();
        assert!(!unsafe {
            ch_lance_export_schema(
                replacement,
                &pinned,
                addr_of_mut!(schema),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });
        assert_eq!(error.kind, ChLanceErrorKind::SnapshotMismatch as u32);
        unsafe { ch_lance_free_error(addr_of_mut!(error)) };

        let mut rows = 0;
        let mut has_value = false;
        assert!(!unsafe {
            ch_lance_total_rows(
                replacement,
                &pinned,
                addr_of_mut!(rows),
                addr_of_mut!(has_value),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });
        assert_eq!(error.kind, ChLanceErrorKind::SnapshotMismatch as u32);
        unsafe { ch_lance_free_error(addr_of_mut!(error)) };

        let mut fragments = ptr::null_mut();
        let mut fragments_size = 0;
        assert!(!unsafe {
            ch_lance_list_fragments(
                replacement,
                &pinned,
                addr_of_mut!(fragments),
                addr_of_mut!(fragments_size),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });
        assert_eq!(error.kind, ChLanceErrorKind::SnapshotMismatch as u32);
        unsafe { ch_lance_free_error(addr_of_mut!(error)) };

        let scan_options = ch_lance_scan_options {
            snapshot: pinned,
            projection: ch_lance_string_list {
                values: ptr::null(),
                size: 0,
            },
            predicate: ptr::null(),
            need_only_count: false,
            max_block_size: 1024,
            limit: 0,
            cancel: ptr::null_mut(),
            scan_unordered: false,
            fragment_readahead: 0,
            batch_readahead: 0,
            io_buffer_size: 0,
            fragment_ids: ptr::null(),
            fragment_ids_size: 0,
        };
        assert!(
            unsafe { ch_lance_plan_scan(replacement, &scan_options, addr_of_mut!(error)) }
                .is_null()
        );
        assert_eq!(error.kind, ChLanceErrorKind::SnapshotMismatch as u32);

        unsafe {
            ch_lance_free_error(addr_of_mut!(error));
            ch_lance_free_dataset(replacement);
        }
    }

    #[test]
    fn snapshot_identity_is_stable_for_local_manifest() {
        let dir = write_test_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = dataset_options(&uri);
        let mut error = empty_error();
        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());

        let mut first = ch_lance_snapshot_info::default();
        let mut second = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(first), addr_of_mut!(error))
        });
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(second), addr_of_mut!(error))
        });
        assert_eq!(first, second);
        assert!(first.manifest_size > 0);

        unsafe { ch_lance_free_dataset(dataset) };
    }

    #[test]
    fn missing_etag_has_zero_digest() {
        let (has_etag, etag_sha256) = snapshot_etag_identity(None);
        assert!(!has_etag);
        assert_eq!(etag_sha256, [0; SNAPSHOT_DIGEST_SIZE]);
    }

    #[test]
    fn manifest_digest_changes_when_raw_bytes_change() {
        assert_ne!(digest_bytes(b"manifest-a"), digest_bytes(b"manifest-b"));
    }

    #[test]
    fn panic_guards_keep_all_ffi_return_classes_inside_rust() {
        assert!(cfg!(panic = "unwind"));

        let mut error = empty_error();
        let mut output = 7_u64;
        let ok = ffi_bool_guard(addr_of_mut!(error), || panic!("bool panic"), || output = 0);
        assert!(!ok);
        assert_eq!(output, 0);
        assert_eq!(error.kind, ChLanceErrorKind::Internal as u32);
        unsafe { ch_lance_free_error(addr_of_mut!(error)) };

        let pointer =
            ffi_ptr_guard::<ch_lance_dataset>(addr_of_mut!(error), || panic!("pointer panic"));
        assert!(pointer.is_null());
        assert_eq!(error.kind, ChLanceErrorKind::Internal as u32);
        unsafe { ch_lance_free_error(addr_of_mut!(error)) };

        ffi_void_guard(|| panic!("void/free panic"));
        let process_continues = 1 + 1;
        assert_eq!(process_continues, 2);

        PANIC_NEXT_FFI_CALL.store(true, Ordering::Release);
        assert!(!unsafe { ch_lance_runtime_ensure(ptr::null(), addr_of_mut!(error)) });
        assert_eq!(error.kind, ChLanceErrorKind::Internal as u32);
        unsafe { ch_lance_free_error(addr_of_mut!(error)) };

        PANIC_NEXT_FFI_CALL.store(true, Ordering::Release);
        assert!(unsafe { ch_lance_cancel_handle_create() }.is_null());

        set_error(
            addr_of_mut!(error),
            FfiError::internal(ChLanceErrorOrigin::Unknown, "owned error"),
        );
        let owned_message = error.message;
        PANIC_NEXT_FFI_CALL.store(true, Ordering::Release);
        unsafe { ch_lance_free_error(addr_of_mut!(error)) };
        assert_eq!(error.message, owned_message);
        unsafe { ch_lance_free_error(addr_of_mut!(error)) };
        assert!(error.message.is_null());
    }

    #[test]
    fn ffi_rejects_zero_dataset_version() {
        let dir = write_test_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = dataset_options(&uri);
        let mut error = empty_error();
        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());

        let mut rows = 0;
        let mut has_value = false;
        let invalid_snapshot = ch_lance_snapshot_info::default();
        assert!(!unsafe {
            ch_lance_total_rows(
                dataset,
                &invalid_snapshot,
                addr_of_mut!(rows),
                addr_of_mut!(has_value),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });
        assert_eq!(error.kind, ChLanceErrorKind::InvalidArgument as u32);
        let message = unsafe { CStr::from_ptr(error.message) }
            .to_str()
            .unwrap()
            .to_string();
        assert!(message.contains("must be non-zero"));

        unsafe {
            ch_lance_free_error(addr_of_mut!(error));
        }

        let mut schema = FFI_ArrowSchema::empty();
        assert!(!unsafe {
            ch_lance_export_schema(
                dataset,
                &invalid_snapshot,
                addr_of_mut!(schema),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });
        assert_eq!(error.kind, ChLanceErrorKind::InvalidArgument as u32);
        let message = unsafe { CStr::from_ptr(error.message) }
            .to_str()
            .unwrap()
            .to_string();
        assert!(message.contains("must be non-zero"));

        unsafe {
            ch_lance_free_error(addr_of_mut!(error));
        }

        let scan_options = ch_lance_scan_options {
            snapshot: invalid_snapshot,
            projection: ch_lance_string_list {
                values: ptr::null(),
                size: 0,
            },
            predicate: ptr::null(),
            need_only_count: false,
            max_block_size: 1024,
            limit: 0,
            cancel: ptr::null_mut(),
            scan_unordered: false,
            fragment_readahead: 0,
            batch_readahead: 0,
            io_buffer_size: 0,
            fragment_ids: ptr::null(),
            fragment_ids_size: 0,
        };
        let scan = unsafe { ch_lance_plan_scan(dataset, &scan_options, addr_of_mut!(error)) };
        assert!(scan.is_null());
        assert_eq!(error.kind, ChLanceErrorKind::InvalidArgument as u32);
        let message = unsafe { CStr::from_ptr(error.message) }
            .to_str()
            .unwrap()
            .to_string();
        assert!(message.contains("must be non-zero"));

        unsafe {
            ch_lance_free_error(addr_of_mut!(error));
            ch_lance_free_dataset(dataset);
        }
    }

    #[test]
    fn ffi_does_not_fall_back_when_dataset_version_is_missing() {
        let dir = write_test_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = dataset_options(&uri);
        let mut error = empty_error();
        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());

        let mut rows = 0;
        let mut has_value = false;
        let mut missing_snapshot = ch_lance_snapshot_info::default();
        missing_snapshot.version = u64::MAX;
        missing_snapshot.manifest_id = [1; SNAPSHOT_DIGEST_SIZE];
        missing_snapshot.manifest_size = 1;
        missing_snapshot.manifest_sha256 = [2; SNAPSHOT_DIGEST_SIZE];
        assert!(!unsafe {
            ch_lance_total_rows(
                dataset,
                &missing_snapshot,
                addr_of_mut!(rows),
                addr_of_mut!(has_value),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });
        assert_eq!(error.kind, ChLanceErrorKind::VersionNotFound as u32);
        assert_eq!(error.origin, ChLanceErrorOrigin::Local as u32);
        let message = unsafe { CStr::from_ptr(error.message) }
            .to_str()
            .unwrap()
            .to_string();
        assert!(message.contains(&u64::MAX.to_string()));
        assert!(!has_value);

        unsafe {
            ch_lance_free_error(addr_of_mut!(error));
            ch_lance_free_dataset(dataset);
        }
    }

    #[test]
    fn ffi_scan_accepts_pushdown_predicates() {
        let dir = write_pushdown_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = dataset_options(&uri);
        let mut error = empty_error();
        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());

        let mut snapshot = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(snapshot), addr_of_mut!(error))
        });

        let projection = [CString::new("id").unwrap()];
        for (predicate, expected_rows) in [
            ("id = 1 OR id = 3", 2),
            ("id IN (1, 3, 5)", 3),
            ("score IS NULL", 2),
            ("event_date = DATE '2024-01-02'", 1),
            ("event_time >= TIMESTAMP '2024-01-02 03:04:05.123'", 4),
        ] {
            let predicate = CString::new(predicate).unwrap();
            assert_eq!(
                scan_row_count(
                    dataset,
                    snapshot,
                    &projection,
                    &predicate,
                    addr_of_mut!(error),
                ),
                expected_rows
            );
        }

        unsafe {
            ch_lance_free_dataset(dataset);
        }
    }

    #[test]
    fn ffi_count_rows_uses_predicate_and_snapshot() {
        let dir = write_pushdown_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = dataset_options(&uri);
        let mut error = empty_error();
        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());

        let mut snapshot = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(snapshot), addr_of_mut!(error))
        });

        let predicate = CString::new("id = 1 OR id = 3").unwrap();
        let mut rows = 0;
        let mut has_value = false;
        assert!(unsafe {
            ch_lance_count_rows(
                dataset,
                &snapshot,
                predicate.as_ptr(),
                ptr::null(),
                0,
                addr_of_mut!(rows),
                addr_of_mut!(has_value),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });
        assert!(has_value);
        assert_eq!(rows, 2);

        let append_batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![1])) as arrow_array::ArrayRef,
            ),
            (
                "name",
                Arc::new(StringArray::from(vec!["new"])) as arrow_array::ArrayRef,
            ),
            (
                "score",
                Arc::new(Float64Array::from(vec![Some(9.0)])) as arrow_array::ArrayRef,
            ),
            (
                "event_date",
                Arc::new(Date32Array::from(vec![19728])) as arrow_array::ArrayRef,
            ),
            (
                "event_time",
                Arc::new(TimestampMillisecondArray::from(vec![1_704_499_200_000]))
                    as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let schema = append_batch.schema();
        let reader = RecordBatchIterator::new(vec![Ok(append_batch)].into_iter(), schema);
        Runtime::new().unwrap().block_on(async {
            let mut dataset = Dataset::open(dir.path().to_str().unwrap()).await.unwrap();
            dataset.append(reader, None).await.unwrap();
        });

        rows = 0;
        has_value = false;
        assert!(unsafe {
            ch_lance_count_rows(
                dataset,
                &snapshot,
                predicate.as_ptr(),
                ptr::null(),
                0,
                addr_of_mut!(rows),
                addr_of_mut!(has_value),
                ptr::null_mut(),
                addr_of_mut!(error),
            )
        });
        assert!(has_value);
        assert_eq!(rows, 2);

        unsafe {
            ch_lance_free_dataset(dataset);
        }
    }

    #[test]
    fn ffi_total_bytes_reports_unavailable() {
        let dir = write_test_dataset();
        let uri = CString::new(dir.path().to_str().unwrap()).unwrap();
        let options = dataset_options(&uri);
        let mut error = empty_error();
        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());

        let mut bytes = 123;
        let mut has_value = true;
        assert!(unsafe {
            ch_lance_total_bytes(
                dataset,
                addr_of_mut!(bytes),
                addr_of_mut!(has_value),
                addr_of_mut!(error),
            )
        });
        assert!(!has_value);

        unsafe {
            ch_lance_free_dataset(dataset);
        }
    }

    #[test]
    fn supported_nested_and_temporal_types_pass_validation() {
        let schema = Schema::new(vec![
            Field::new(
                "m",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("keys", DataType::Utf8, false),
                                Field::new("values", DataType::Int32, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    true,
                ),
                true,
            ),
            Field::new(
                "nested",
                DataType::Struct(
                    vec![
                        Field::new(
                            "values",
                            DataType::List(Arc::new(Field::new(
                                "item",
                                DataType::Decimal128(18, 4),
                                true,
                            ))),
                            true,
                        ),
                        Field::new(
                            "event_time",
                            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                            true,
                        ),
                        Field::new("duration", DataType::Duration(TimeUnit::Microsecond), false),
                    ]
                    .into(),
                ),
                true,
            ),
            Field::new(
                "fixed",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 3),
                false,
            ),
        ]);

        assert!(validate_schema(&schema, ChLanceErrorOrigin::Local).is_ok());
    }

    #[test]
    fn nested_extension_type_reports_full_path() {
        let mut metadata = HashMap::new();
        metadata.insert(
            "ARROW:extension:name".to_string(),
            "example.extension".to_string(),
        );
        let extension_field = Field::new("value", DataType::Int32, true).with_metadata(metadata);
        let schema = Schema::new(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(vec![extension_field].into()),
                true,
            ))),
            true,
        )]);

        let error = validate_schema(&schema, ChLanceErrorOrigin::Local).unwrap_err();
        assert_eq!(error.kind, ChLanceErrorKind::Unsupported);
        assert!(error
            .message
            .contains("Unsupported Lance column `items[].value`"));
        assert!(error.message.contains("extension types"));
    }

    #[test]
    fn date64_reports_lossless_mapping_error() {
        let schema = Schema::new(vec![Field::new("event_date", DataType::Date64, true)]);

        let error = validate_schema(&schema, ChLanceErrorOrigin::Local).unwrap_err();
        assert_eq!(error.kind, ChLanceErrorKind::Unsupported);
        assert!(error
            .message
            .contains("Unsupported Lance column `event_date`"));
        assert!(error.message.contains("lossless ClickHouse mapping"));
    }

    #[test]
    fn nullable_map_key_reports_full_path() {
        let schema = Schema::new(vec![Field::new(
            "attributes",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("keys", DataType::Utf8, true),
                            Field::new("values", DataType::Int32, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        )]);

        let error = validate_schema(&schema, ChLanceErrorOrigin::Local).unwrap_err();
        assert_eq!(error.kind, ChLanceErrorKind::Unsupported);
        assert!(error
            .message
            .contains("Unsupported Lance column `attributes.key`"));
        assert!(error.message.contains("map keys must not be nullable"));
    }

    fn object_store_error(kind: ChLanceErrorKind) -> object_store::Error {
        let source = || {
            Box::new(std::io::Error::other("test object store error"))
                as Box<dyn StdError + Send + Sync>
        };
        match kind {
            ChLanceErrorKind::NotFound => object_store::Error::NotFound {
                path: "dataset/_latest.manifest".to_string(),
                source: source(),
            },
            ChLanceErrorKind::PermissionDenied => object_store::Error::PermissionDenied {
                path: "dataset/_latest.manifest".to_string(),
                source: source(),
            },
            ChLanceErrorKind::Unauthenticated => object_store::Error::Unauthenticated {
                path: "dataset/_latest.manifest".to_string(),
                source: source(),
            },
            _ => object_store::Error::Generic {
                store: "test",
                source: source(),
            },
        }
    }

    #[test]
    fn ffi_error_discriminants_are_stable() {
        assert_eq!(ChLanceErrorKind::None as u32, 0);
        assert_eq!(ChLanceErrorKind::InvalidArgument as u32, 1);
        assert_eq!(ChLanceErrorKind::NotFound as u32, 2);
        assert_eq!(ChLanceErrorKind::PermissionDenied as u32, 3);
        assert_eq!(ChLanceErrorKind::Unauthenticated as u32, 4);
        assert_eq!(ChLanceErrorKind::CorruptData as u32, 5);
        assert_eq!(ChLanceErrorKind::Unsupported as u32, 6);
        assert_eq!(ChLanceErrorKind::VersionNotFound as u32, 7);
        assert_eq!(ChLanceErrorKind::Storage as u32, 8);
        assert_eq!(ChLanceErrorKind::Internal as u32, 9);
        assert_eq!(ChLanceErrorOrigin::Unknown as u32, 0);
        assert_eq!(ChLanceErrorOrigin::Local as u32, 1);
        assert_eq!(ChLanceErrorOrigin::S3 as u32, 2);
    }

    #[test]
    fn classifies_nested_object_store_errors() {
        for expected in [
            ChLanceErrorKind::NotFound,
            ChLanceErrorKind::PermissionDenied,
            ChLanceErrorKind::Unauthenticated,
        ] {
            let lance_error: lance::Error = object_store_error(expected).into();
            assert_eq!(
                classify_lance_error(&lance_error, LanceOperation::Open),
                expected
            );
        }

        let lance_error: lance::Error = object_store_error(ChLanceErrorKind::Storage).into();
        assert_eq!(
            classify_lance_error(&lance_error, LanceOperation::Open),
            ChLanceErrorKind::Storage
        );
    }

    #[test]
    fn classifies_dataset_not_found_from_nested_error() {
        for expected in [
            ChLanceErrorKind::NotFound,
            ChLanceErrorKind::PermissionDenied,
            ChLanceErrorKind::Unauthenticated,
            ChLanceErrorKind::Storage,
        ] {
            let source = object_store_error(expected);
            assert_eq!(
                classify_dataset_not_found(&source, LanceOperation::Open),
                expected
            );
        }
    }

    #[test]
    fn classifies_arrow_decode_errors_as_corrupt_data() {
        let lance_error: lance::Error =
            arrow_schema::ArrowError::ParseError("invalid dataset metadata".to_string()).into();
        assert_eq!(
            classify_lance_error(&lance_error, LanceOperation::Open),
            ChLanceErrorKind::CorruptData
        );

        let mut invalid_varint = &[0x80_u8][..];
        let decode_error = prost::encoding::decode_varint(&mut invalid_varint).unwrap_err();
        let lance_error: lance::Error = decode_error.into();
        assert_eq!(
            classify_lance_error(&lance_error, LanceOperation::Open),
            ChLanceErrorKind::CorruptData
        );
    }

    #[test]
    fn preserves_kind_when_error_message_contains_null() {
        let mut error = empty_error();
        set_error(
            addr_of_mut!(error),
            FfiError::new(
                ChLanceErrorKind::Unsupported,
                ChLanceErrorOrigin::Local,
                "unsupported\0column",
            ),
        );

        assert_eq!(error.kind, ChLanceErrorKind::Unsupported as u32);
        assert_eq!(error.origin, ChLanceErrorOrigin::Local as u32);
        assert_eq!(
            unsafe { CStr::from_ptr(error.message) }.to_str().unwrap(),
            "Lance error contains an interior null byte"
        );

        unsafe {
            ch_lance_free_error(addr_of_mut!(error));
        }
        assert_eq!(error.kind, ChLanceErrorKind::None as u32);
        assert_eq!(error.origin, ChLanceErrorOrigin::Unknown as u32);
        assert!(error.message.is_null());
    }

    #[test]
    fn redacts_uri_user_info() {
        assert_eq!(
            redact_uri_user_info(
                "Cannot open https://alice:secret@example.com/dataset".to_string()
            ),
            "Cannot open https://<redacted>@example.com/dataset"
        );
    }

    #[test]
    fn open_s3_dataset_from_env_when_configured() {
        let Ok(uri) = std::env::var("CH_LANCE_TEST_S3_URI") else {
            return;
        };

        let mut storage_options = HashMap::new();
        for (env_name, option_name) in [
            ("CH_LANCE_TEST_S3_ENDPOINT", "aws_endpoint"),
            ("CH_LANCE_TEST_S3_REGION", "aws_region"),
            ("CH_LANCE_TEST_S3_ACCESS_KEY_ID", "aws_access_key_id"),
            (
                "CH_LANCE_TEST_S3_SECRET_ACCESS_KEY",
                "aws_secret_access_key",
            ),
        ] {
            if let Ok(value) = std::env::var(env_name) {
                storage_options.insert(option_name.to_string(), value);
            }
        }
        storage_options.insert("aws_allow_http".to_string(), "true".to_string());
        storage_options.insert(
            "aws_virtual_hosted_style_request".to_string(),
            "false".to_string(),
        );

        let runtime = Runtime::new().unwrap();
        let opened = runtime
            .block_on(open_dataset(DatasetOpenOptions {
                uri,
                storage_options: Some(storage_options),
                origin: ChLanceErrorOrigin::S3,
            }))
            .unwrap();
        let rows = runtime.block_on(opened.dataset.count_rows(None)).unwrap();
        assert_eq!(rows, 3);

        let scan_rows = runtime.block_on(async {
            let mut stream = opened.dataset.scan().try_into_stream().await.unwrap();
            let mut rows = 0;
            while let Some(batch) = stream.next().await {
                rows += batch.unwrap().num_rows();
            }
            rows
        });
        assert_eq!(scan_rows, 3);

        let uri = CString::new(std::env::var("CH_LANCE_TEST_S3_URI").unwrap()).unwrap();
        let endpoint = CString::new(std::env::var("CH_LANCE_TEST_S3_ENDPOINT").unwrap()).unwrap();
        let region = CString::new(
            std::env::var("CH_LANCE_TEST_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
        )
        .unwrap();
        let access_key =
            CString::new(std::env::var("CH_LANCE_TEST_S3_ACCESS_KEY_ID").unwrap()).unwrap();
        let secret_key =
            CString::new(std::env::var("CH_LANCE_TEST_S3_SECRET_ACCESS_KEY").unwrap()).unwrap();
        let options = ch_lance_dataset_options {
            uri: uri.as_ptr(),
            use_s3: true,
            s3_region: region.as_ptr(),
            s3_endpoint: endpoint.as_ptr(),
            s3_access_key_id: access_key.as_ptr(),
            s3_secret_access_key: secret_key.as_ptr(),
            s3_session_token: ptr::null(),
            s3_role_arn: ptr::null(),
            s3_role_session_name: ptr::null(),
            s3_use_environment_credentials: false,
            s3_no_sign_request: false,
            s3_allow_http: true,
            s3_virtual_hosted_style_request: false,
            s3_request_timeout_ms: 0,
            s3_connect_timeout_ms: 0,
            cancel: ptr::null_mut(),
        };
        let mut error = empty_error();
        let dataset = unsafe { ch_lance_open_dataset(&options, addr_of_mut!(error)) };
        assert!(!dataset.is_null());

        let mut snapshot = ch_lance_snapshot_info::default();
        assert!(unsafe {
            ch_lance_current_snapshot(dataset, addr_of_mut!(snapshot), addr_of_mut!(error))
        });
        let projection = [
            CString::new("id").unwrap(),
            CString::new("name").unwrap(),
            CString::new("score").unwrap(),
        ];
        let projection_ptrs = projection
            .iter()
            .map(|value| value.as_ptr())
            .collect::<Vec<_>>();
        let scan_options = ch_lance_scan_options {
            snapshot,
            projection: ch_lance_string_list {
                values: projection_ptrs.as_ptr(),
                size: projection_ptrs.len(),
            },
            predicate: ptr::null(),
            need_only_count: false,
            max_block_size: 8192,
            limit: 0,
            cancel: ptr::null_mut(),
            scan_unordered: false,
            fragment_readahead: 0,
            batch_readahead: 0,
            io_buffer_size: 0,
            fragment_ids: ptr::null(),
            fragment_ids_size: 0,
        };
        let scan = unsafe { ch_lance_plan_scan(dataset, &scan_options, addr_of_mut!(error)) };
        assert!(!scan.is_null());
        let mut ffi_rows = 0;
        loop {
            let mut array = FFI_ArrowArray::empty();
            let mut schema = FFI_ArrowSchema::empty();
            let mut has_batch = false;
            assert!(unsafe {
                ch_lance_next_batch(
                    scan,
                    addr_of_mut!(array),
                    addr_of_mut!(schema),
                    addr_of_mut!(has_batch),
                    addr_of_mut!(error),
                )
            });
            if !has_batch {
                break;
            }
            let struct_data = unsafe { from_ffi(array, &schema) }.unwrap();
            ffi_rows += StructArray::from(make_array(struct_data).to_data()).len();
        }
        assert_eq!(ffi_rows, 3);
        unsafe {
            ch_lance_free_scan(scan);
            ch_lance_free_dataset(dataset);
        }
    }

    /// Generate `tests/queries/0_stateless/data_lance/multi_frag.lance` when
    /// `LANCE_MULTI_FRAG_OUT` is set to an absolute or relative output path.
    ///
    /// ```text
    /// LANCE_MULTI_FRAG_OUT=../../tests/queries/0_stateless/data_lance/multi_frag.lance \
    ///   cargo test -p _ch_rust_lance write_stateless_multi_frag_fixture -- --exact --nocapture
    /// ```
    #[test]
    fn write_stateless_multi_frag_fixture() {
        use lance::dataset::WriteParams;
        use std::fs;
        use std::path::PathBuf;

        let Ok(out) = std::env::var("LANCE_MULTI_FRAG_OUT") else {
            return;
        };
        let out_path = PathBuf::from(&out);
        if out_path.exists() {
            fs::remove_dir_all(&out_path).expect("remove existing multi_frag.lance");
        }
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent).expect("create parent for multi_frag.lance");
        }

        // 64 rows, max_rows_per_file=8 → 8 fragments.
        let ids: Vec<i64> = (1..=64).collect();
        let names: Vec<String> = ids.iter().map(|id| format!("n{}", id)).collect();
        let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(arrow_array::Int64Array::from(ids)) as arrow_array::ArrayRef,
            ),
            (
                "name",
                Arc::new(StringArray::from(name_refs)) as arrow_array::ArrayRef,
            ),
        ])
        .unwrap();
        let schema = batch.schema();
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
        let params = WriteParams {
            max_rows_per_file: 8,
            ..Default::default()
        };
        Runtime::new()
            .unwrap()
            .block_on(Dataset::write(
                reader,
                out_path.to_str().unwrap(),
                Some(params),
            ))
            .expect("write multi_frag.lance");

        let ds = Runtime::new()
            .unwrap()
            .block_on(Dataset::open(out_path.to_str().unwrap()))
            .expect("reopen multi_frag.lance");
        let n_frags = ds.get_fragments().len();
        assert!(
            n_frags >= 8,
            "expected at least 8 fragments, got {}",
            n_frags
        );
        eprintln!(
            "Wrote multi_frag.lance to {} with {} fragments",
            out_path.display(),
            n_frags
        );
    }
}
