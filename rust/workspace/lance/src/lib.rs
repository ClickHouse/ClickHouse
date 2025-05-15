use std::ffi::{c_void, CStr, CString};
use std::os::raw::c_char;
use std::sync::Arc;

use arrow_array::types::{
    Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, PrimitiveArray, RecordBatch, RecordBatchIterator,
    StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use futures::stream::{BoxStream, StreamExt};

use lancedb::connect;
use lancedb::connection::Connection;
use lancedb::query::ExecutableQuery;
use lancedb::table::{ColumnAlteration, Table};

#[repr(u8)]
#[derive(Copy, Clone, PartialEq)]
pub enum RustLanceColumnType {
    Unknown = 0,
    Int8 = 1,
    UInt8 = 2,
    Int16 = 3,
    UInt16 = 4,
    Int32 = 5,
    UInt32 = 6,
    Int64 = 7,
    UInt64 = 8,
    Float32 = 9,
    Float64 = 10,
    String = 11,
}

#[repr(C)]
pub struct RustLanceReader {
    table: *mut RustLanceTable,
    stream: BoxStream<'static, lancedb::Result<RecordBatch>>,
    current: Option<RecordBatch>,
}

#[repr(C)]
pub struct RustLanceDatabase {
    connection: Connection,
}

#[repr(C)]
pub struct RustLanceTable {
    rt: tokio::runtime::Runtime,
    table: Option<Table>,
}

#[repr(C)]
pub struct RustLanceColumn {
    pub data_type: RustLanceColumnType,
    pub data: *mut c_void,
    pub len: usize,
    pub capacity: usize,
}

#[repr(C)]
pub struct NullableRustLanceColumn {
    pub column: RustLanceColumn,
    pub nulls: *mut bool,
}

#[repr(C)]
pub struct RustLanceColumnDescription {
    pub name: *mut c_char,
    pub data_type: RustLanceColumnType,
    pub is_nullable: bool,
}

#[repr(C)]
pub struct RustLanceSchema {
    pub data: *mut RustLanceColumnDescription,
    pub len: usize,
    pub capacity: usize,
}

#[repr(C)]
pub struct RustLanceBatch {
    pub schema: Option<Schema>,
    pub columns: Vec<ArrayRef>,
}

#[no_mangle]
pub extern "C" fn rust_connect_to_database(database_path: *const c_char) -> *mut RustLanceDatabase {
    let path_str = unsafe { CStr::from_ptr(database_path).to_str().unwrap() };
    let rt = tokio::runtime::Runtime::new().unwrap();
    match rt.block_on(connect(path_str).execute()) {
        Ok(connection) => Box::into_raw(Box::new(RustLanceDatabase { connection })),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn rust_free_lance_database(database: *mut RustLanceDatabase) {
    unsafe {
        let _ = Box::from_raw(database);
    }
}

#[no_mangle]
pub extern "C" fn rust_try_open_table(
    lance_database: *mut RustLanceDatabase,
    table_name: *const c_char,
) -> *mut RustLanceTable {
    let lance_database = unsafe { &mut *lance_database };
    let db = &lance_database.connection;
    let name_str = unsafe { CStr::from_ptr(table_name).to_str().unwrap() };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let table = rt.block_on(db.open_table(name_str).execute());

    match table {
        Ok(result) => Box::into_raw(Box::new(RustLanceTable {
            rt,
            table: Some(result),
        })),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn rust_create_table_with_schema(
    lance_database: *mut RustLanceDatabase,
    table_name: *const c_char,
    schema: RustLanceSchema,
) -> *mut RustLanceTable {
    let lance_database = unsafe { &mut *lance_database };
    let db = &lance_database.connection;
    let name_str = unsafe { CStr::from_ptr(table_name).to_str().unwrap() };

    let rt = tokio::runtime::Runtime::new().unwrap();
    let schema = Arc::new(c_lance_schema_to_arrow_schema(schema));

    let table = rt.block_on(db.create_empty_table(name_str, schema.clone()).execute());

    match table {
        Ok(result) => Box::into_raw(Box::new(RustLanceTable {
            rt,
            table: Some(result),
        })),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn rust_free_lance_table(table: *mut RustLanceTable) {
    unsafe {
        let _ = Box::from_raw(table);
    }
}

#[no_mangle]
pub extern "C" fn rust_drop_lance_table(
    lance_database: *mut RustLanceDatabase,
    table_name: *const c_char,
) -> bool {
    let lance_database = unsafe { &mut *lance_database };
    let db = &lance_database.connection;
    let name_str = unsafe { CStr::from_ptr(table_name).to_str().unwrap() };

    let rt = tokio::runtime::Runtime::new().unwrap();
    match rt.block_on(db.drop_table(name_str)) {
        Ok(()) => true,
        Err(_) => false,
    }
}

#[no_mangle]
pub extern "C" fn rust_create_lance_reader(
    lance_table_ptr: *mut RustLanceTable,
) -> *mut RustLanceReader {
    let lance_table = unsafe { &mut *lance_table_ptr };
    let tbl = lance_table.table.as_ref().unwrap();
    let stream = lance_table.rt.block_on(tbl.query().execute()).unwrap();

    let reader = RustLanceReader {
        table: lance_table_ptr,
        stream,
        current: None,
    };
    Box::into_raw(Box::new(reader))
}

#[no_mangle]
pub extern "C" fn rust_free_lance_reader(reader: *mut RustLanceReader) {
    unsafe {
        let _ = Box::from_raw(reader);
    }
}

#[no_mangle]
pub extern "C" fn rust_read_next_batch(reader: *mut RustLanceReader) -> bool {
    let reader = unsafe { &mut *reader };
    let lance_table = unsafe { &mut *reader.table };

    let next = lance_table.rt.block_on(reader.stream.next());

    match next {
        Some(Ok(batch)) => {
            reader.current = Some(batch);
            true
        }
        _ => false,
    }
}

#[no_mangle]
pub extern "C" fn rust_get_rows_in_current_batch(reader: *mut RustLanceReader) -> usize {
    let reader = unsafe { &mut *reader };
    match &reader.current {
        Some(batch) => batch.num_rows(),
        None => 0,
    }
}

#[no_mangle]
pub extern "C" fn rust_get_lance_column(
    reader: *mut RustLanceReader,
    col: usize,
) -> RustLanceColumn {
    let reader = unsafe { &mut *reader };
    let batch = reader.current.as_ref().unwrap();
    let array = batch.column(col);
    array_ref_to_lance_column(array)
}

#[no_mangle]
pub extern "C" fn rust_get_nullable_lance_column(
    reader_ptr: *mut RustLanceReader,
    col: usize,
) -> NullableRustLanceColumn {
    let reader = unsafe { &mut *reader_ptr };
    let batch = reader.current.as_ref().unwrap();
    let array = batch.column(col);

    if !array.is_nullable() {
        return NullableRustLanceColumn {
            column: rust_get_lance_column(reader_ptr, col),
            nulls: std::ptr::null_mut(),
        };
    }

    let mut nulls: Vec<bool> = Vec::with_capacity(array.len());
    for i in 0..array.len() {
        nulls.push(array.is_null(i));
    }
    let column = array_ref_to_lance_column(array);
    let result = NullableRustLanceColumn {
        column,
        nulls: nulls.as_mut_ptr(),
    };
    std::mem::forget(nulls);
    result
}

pub fn parse_data_type(data_type: &DataType) -> RustLanceColumnType {
    match data_type {
        DataType::Int8 => RustLanceColumnType::Int8,
        DataType::UInt8 => RustLanceColumnType::UInt8,
        DataType::Int16 => RustLanceColumnType::Int16,
        DataType::UInt16 => RustLanceColumnType::UInt16,
        DataType::Int32 => RustLanceColumnType::Int32,
        DataType::UInt32 => RustLanceColumnType::UInt32,
        DataType::Int64 => RustLanceColumnType::Int64,
        DataType::UInt64 => RustLanceColumnType::UInt64,
        DataType::Float32 => RustLanceColumnType::Float32,
        DataType::Float64 => RustLanceColumnType::Float64,
        DataType::Utf8 => RustLanceColumnType::String,
        _ => unimplemented!(),
    }
}

pub fn parse_lance_column_type(lance_column_type: &RustLanceColumnType) -> DataType {
    match lance_column_type {
        RustLanceColumnType::Int8 => DataType::Int8,
        RustLanceColumnType::UInt8 => DataType::UInt8,
        RustLanceColumnType::Int16 => DataType::Int16,
        RustLanceColumnType::UInt16 => DataType::UInt16,
        RustLanceColumnType::Int32 => DataType::Int32,
        RustLanceColumnType::UInt32 => DataType::UInt32,
        RustLanceColumnType::Int64 => DataType::Int64,
        RustLanceColumnType::UInt64 => DataType::UInt64,
        RustLanceColumnType::Float32 => DataType::Float32,
        RustLanceColumnType::Float64 => DataType::Float64,
        RustLanceColumnType::String => DataType::Utf8,
        _ => unimplemented!(),
    }
}

pub fn string_to_c_char(string: String) -> *mut c_char {
    let cstr = CString::new(string).unwrap();
    let cchar: *mut c_char = cstr.into_raw();
    cchar
}

pub fn free_c_char(cchar: *mut c_char) {
    unsafe {
        let _ = CString::from_raw(cchar);
    };
}

#[no_mangle]
pub extern "C" fn rust_read_lance_schema(lance_table: *mut RustLanceTable) -> RustLanceSchema {
    let lance_table = unsafe { &*lance_table };
    let schema: SchemaRef = lance_table
        .rt
        .block_on(lance_table.table.as_ref().unwrap().schema())
        .unwrap();
    let mut values: Vec<RustLanceColumnDescription> = schema
        .fields()
        .iter()
        .map(|field| RustLanceColumnDescription {
            name: string_to_c_char(field.name().as_str().to_string()),
            data_type: parse_data_type(field.data_type()),
            is_nullable: field.is_nullable(),
        })
        .collect();

    let buf = RustLanceSchema {
        data: values.as_mut_ptr(),
        len: values.len(),
        capacity: values.capacity(),
    };

    std::mem::forget(values);
    buf
}

#[no_mangle]
pub extern "C" fn rust_free_lance_schema(buf: RustLanceSchema) {
    unsafe {
        let ptrs = Vec::from_raw_parts(buf.data, buf.len, buf.capacity);
        for p in &ptrs {
            free_c_char(p.name);
        }
    }
}

pub fn is_numeric_type(lance_type: RustLanceColumnType) -> bool {
    return lance_type == RustLanceColumnType::Int8
        || lance_type == RustLanceColumnType::UInt8
        || lance_type == RustLanceColumnType::Int16
        || lance_type == RustLanceColumnType::UInt16
        || lance_type == RustLanceColumnType::Int32
        || lance_type == RustLanceColumnType::UInt32
        || lance_type == RustLanceColumnType::Int64
        || lance_type == RustLanceColumnType::UInt64
        || lance_type == RustLanceColumnType::Float32
        || lance_type == RustLanceColumnType::Float64;
}

#[no_mangle]
pub extern "C" fn rust_free_lance_column(column: RustLanceColumn) {
    // numeric lance columns are just views, so no need to free
    if is_numeric_type(column.data_type) {
        return;
    }
    if column.data_type == RustLanceColumnType::String {
        let strings = vec_from_lance_column::<*mut c_char>(column);
        for s in strings {
            if s != std::ptr::null_mut() {
                free_c_char(s);
            }
        }
    } else {
        unimplemented!();
    }
}

#[no_mangle]
pub extern "C" fn rust_free_nullable_lance_column(column: NullableRustLanceColumn) {
    if column.nulls != std::ptr::null_mut() {
        unsafe { Vec::from_raw_parts(column.nulls, column.column.len, column.column.len) };
    }
    rust_free_lance_column(column.column);
}

pub fn array_ref_to_lance_column(array: &ArrayRef) -> RustLanceColumn {
    match array.data_type() {
        DataType::Int8 => numeric_array_ref_to_lance_column::<Int8Type>(&array),
        DataType::UInt8 => numeric_array_ref_to_lance_column::<UInt8Type>(&array),
        DataType::Int16 => numeric_array_ref_to_lance_column::<Int16Type>(&array),
        DataType::UInt16 => numeric_array_ref_to_lance_column::<UInt16Type>(&array),
        DataType::Int32 => numeric_array_ref_to_lance_column::<Int32Type>(&array),
        DataType::UInt32 => numeric_array_ref_to_lance_column::<UInt32Type>(&array),
        DataType::Int64 => numeric_array_ref_to_lance_column::<Int64Type>(&array),
        DataType::UInt64 => numeric_array_ref_to_lance_column::<UInt64Type>(&array),
        DataType::Float32 => numeric_array_ref_to_lance_column::<Float32Type>(&array),
        DataType::Float64 => numeric_array_ref_to_lance_column::<Float64Type>(&array),
        DataType::Utf8 => {
            let mut vec: Vec<_> = array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|string| match string {
                    Some(string) => string_to_c_char(string.to_string()),
                    None => std::ptr::null_mut(),
                })
                .collect();

            let column = RustLanceColumn {
                data_type: RustLanceColumnType::String,
                data: vec.as_mut_ptr() as *mut c_void,
                len: vec.len(),
                capacity: vec.capacity(),
            };

            std::mem::forget(vec);
            column
        }
        _ => unimplemented!(),
    }
}

pub fn numeric_array_ref_to_lance_column<T: ArrowPrimitiveType>(
    array: &ArrayRef,
) -> RustLanceColumn {
    let ptr: *const <T as ArrowPrimitiveType>::Native = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .unwrap()
        .values()
        .as_ptr();

    let column = RustLanceColumn {
        data_type: parse_data_type(array.data_type()),
        data: ptr as *mut c_void,
        len: array.len(),
        capacity: array.len(),
    };

    // std::mem::forget(vec);
    column
}

pub fn lance_column_to_array_ref(column: RustLanceColumn) -> ArrayRef {
    match column.data_type {
        RustLanceColumnType::Int8 => numeric_lance_column_to_array_ref::<Int8Type>(column),
        RustLanceColumnType::UInt8 => numeric_lance_column_to_array_ref::<UInt8Type>(column),
        RustLanceColumnType::Int16 => numeric_lance_column_to_array_ref::<Int16Type>(column),
        RustLanceColumnType::UInt16 => numeric_lance_column_to_array_ref::<UInt16Type>(column),
        RustLanceColumnType::Int32 => numeric_lance_column_to_array_ref::<Int32Type>(column),
        RustLanceColumnType::UInt32 => numeric_lance_column_to_array_ref::<UInt32Type>(column),
        RustLanceColumnType::Int64 => numeric_lance_column_to_array_ref::<Int64Type>(column),
        RustLanceColumnType::UInt64 => numeric_lance_column_to_array_ref::<UInt64Type>(column),
        RustLanceColumnType::Float32 => numeric_lance_column_to_array_ref::<Float32Type>(column),
        RustLanceColumnType::Float64 => numeric_lance_column_to_array_ref::<Float64Type>(column),
        RustLanceColumnType::String => {
            let vec = vec_from_lance_column::<*mut c_char>(column);
            let result: StringArray = vec
                .iter()
                .map(|cchar| unsafe { Some(CStr::from_ptr(cchar.clone()).to_str().unwrap()) })
                .collect();
            std::mem::forget(vec);
            Arc::new(result)
        }
        _ => unimplemented!(),
    }
}

pub fn nullable_lance_column_to_array_ref(column: NullableRustLanceColumn) -> ArrayRef {
    if column.nulls == std::ptr::null_mut() {
        return lance_column_to_array_ref(column.column);
    }
    match column.column.data_type {
        RustLanceColumnType::Int8 => numeric_nullable_lance_column_to_array_ref::<Int8Type>(column),
        RustLanceColumnType::UInt8 => {
            numeric_nullable_lance_column_to_array_ref::<UInt8Type>(column)
        }
        RustLanceColumnType::Int16 => {
            numeric_nullable_lance_column_to_array_ref::<Int16Type>(column)
        }
        RustLanceColumnType::UInt16 => {
            numeric_nullable_lance_column_to_array_ref::<UInt16Type>(column)
        }
        RustLanceColumnType::Int32 => {
            numeric_nullable_lance_column_to_array_ref::<Int32Type>(column)
        }
        RustLanceColumnType::UInt32 => {
            numeric_nullable_lance_column_to_array_ref::<UInt32Type>(column)
        }
        RustLanceColumnType::Int64 => {
            numeric_nullable_lance_column_to_array_ref::<Int64Type>(column)
        }
        RustLanceColumnType::UInt64 => {
            numeric_nullable_lance_column_to_array_ref::<UInt64Type>(column)
        }
        RustLanceColumnType::Float32 => {
            numeric_nullable_lance_column_to_array_ref::<Float32Type>(column)
        }
        RustLanceColumnType::Float64 => {
            numeric_nullable_lance_column_to_array_ref::<Float64Type>(column)
        }
        RustLanceColumnType::String => {
            let vec = vec_from_lance_column::<*mut c_char>(column.column);
            let result: StringArray = vec
                .iter()
                .map(|cchar| {
                    if *cchar == std::ptr::null_mut() {
                        None
                    } else {
                        unsafe { Some(CStr::from_ptr(cchar.clone()).to_str().unwrap()) }
                    }
                })
                .collect();
            std::mem::forget(vec);
            Arc::new(result)
        }
        _ => unimplemented!(),
    }
}

pub fn numeric_nullable_lance_column_to_array_ref<T: ArrowPrimitiveType>(
    column: NullableRustLanceColumn,
) -> ArrayRef
where
    T::Native: Clone,
    PrimitiveArray<T>: From<Vec<Option<<T as ArrowPrimitiveType>::Native>>>,
{
    let nulls = unsafe { Vec::from_raw_parts(column.nulls, column.column.len, column.column.len) };
    let vec = vec_from_lance_column::<T::Native>(column.column);

    let data: Vec<_> = vec
        .iter()
        .zip(nulls.iter())
        .map(|(value, is_null)| if *is_null { None } else { Some(value.clone()) })
        .collect();

    std::mem::forget(nulls);
    std::mem::forget(vec);

    Arc::new(PrimitiveArray::<T>::from(data))
}

pub fn numeric_lance_column_to_array_ref<T: ArrowPrimitiveType>(column: RustLanceColumn) -> ArrayRef
where
    T::Native: Clone,
    PrimitiveArray<T>: From<Vec<<T as ArrowPrimitiveType>::Native>>,
{
    let vec = vec_from_lance_column::<T::Native>(column);
    let result = Arc::new(PrimitiveArray::<T>::from(vec.clone()));
    std::mem::forget(vec);
    result
}

pub fn vec_from_lance_column<T>(column: RustLanceColumn) -> Vec<T> {
    unsafe { Vec::from_raw_parts(column.data as *mut T, column.len, column.capacity) }
}

#[no_mangle]
pub extern "C" fn rust_create_lance_batch() -> *mut RustLanceBatch {
    let batch = RustLanceBatch {
        schema: None,
        columns: Vec::new(),
    };
    Box::into_raw(Box::new(batch))
}

#[no_mangle]
pub extern "C" fn rust_free_lance_batch(batch: *mut RustLanceBatch) {
    unsafe {
        let _ = Box::from_raw(batch);
    }
}

pub fn c_lance_schema_to_arrow_schema(schema: RustLanceSchema) -> Schema {
    unsafe {
        let lance_fields = Vec::from_raw_parts(schema.data, schema.len, schema.capacity);
        let mut fields: Vec<Field> = Vec::new();
        for lance_field in &lance_fields {
            let name = CStr::from_ptr(lance_field.name).to_str().unwrap();
            let data_type = parse_lance_column_type(&lance_field.data_type);
            fields.push(Field::new(name, data_type, lance_field.is_nullable));
        }
        std::mem::forget(lance_fields);
        Schema::new(fields)
    }
}

#[no_mangle]
pub extern "C" fn rust_set_schema_for_lance_batch(
    batch: *mut RustLanceBatch,
    schema: RustLanceSchema,
) {
    let batch = unsafe { &mut *batch };
    batch.schema = Some(c_lance_schema_to_arrow_schema(schema));
}

#[no_mangle]
pub extern "C" fn rust_append_column_to_lance_batch(
    batch: *mut RustLanceBatch,
    column: RustLanceColumn,
) {
    let batch = unsafe { &mut *batch };
    batch.columns.push(lance_column_to_array_ref(column));
}

#[no_mangle]
pub extern "C" fn rust_append_nullable_column_to_lance_batch(
    batch: *mut RustLanceBatch,
    column: NullableRustLanceColumn,
) {
    let batch = unsafe { &mut *batch };
    batch
        .columns
        .push(nullable_lance_column_to_array_ref(column));
}

#[no_mangle]
pub extern "C" fn rust_write_batch_to_lance_table(
    lance_table: *mut RustLanceTable,
    batch: *mut RustLanceBatch,
) {
    let batch = unsafe { &mut *batch };
    let batch_to_insert = RecordBatch::try_new(
        Arc::new(batch.schema.clone().unwrap()),
        batch.columns.clone(),
    )
    .unwrap();

    let reader = RecordBatchIterator::new(
        std::iter::once(Ok(batch_to_insert.clone())),
        batch_to_insert.schema().clone(),
    );

    let lance_table = unsafe { &mut *lance_table };

    lance_table
        .rt
        .block_on(lance_table.table.as_ref().unwrap().add(reader).execute())
        .unwrap();
}

#[no_mangle]
pub extern "C" fn rust_rename_column(
    lance_table: *mut RustLanceTable,
    old_name: *const c_char,
    new_name: *const c_char,
) {
    let old_name_str = unsafe { CStr::from_ptr(old_name).to_str().unwrap() };
    let new_name_str = unsafe { CStr::from_ptr(new_name).to_str().unwrap() };
    let lance_table = unsafe { &mut *lance_table };

    let alteration =
        ColumnAlteration::new(old_name_str.to_string()).rename(new_name_str.to_string());

    lance_table
        .rt
        .block_on(
            lance_table
                .table
                .as_ref()
                .unwrap()
                .alter_columns(&[alteration]),
        )
        .unwrap();
}

#[no_mangle]
pub extern "C" fn rust_drop_column(lance_table: *mut RustLanceTable, column_name: *const c_char) {
    let column_name = unsafe { CStr::from_ptr(column_name).to_str().unwrap() };
    let lance_table = unsafe { &mut *lance_table };

    lance_table
        .rt
        .block_on(
            lance_table
                .table
                .as_ref()
                .unwrap()
                .drop_columns(&[column_name]),
        )
        .unwrap();
}
