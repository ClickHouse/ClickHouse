use pco::data_types::{CoreDataType, NumberLike};

use crate::PcoError::{PcoSuccess, PcoCompressionError, PcoInvalidType};

use pco::DEFAULT_MAX_PAGE_N;

#[repr(C)]
pub enum PcoError {
  PcoSuccess,
  PcoInvalidType,
  PcoInvalidArgument,
  PcoCompressionError,
  PcoDecompressionError,
}

macro_rules! impl_dtypes {
  {$($names:ident($lname:ident) => $types:ty,)+} => {
    macro_rules! match_dtype {
      ($matcher:expr, $fn:ident, $params:tt) => {
        match $matcher {
          $(CoreDataType::$names => $fn::<$types>$params,)+
        }
      }
    }
  }
}

pco::with_core_dtypes!(impl_dtypes);

fn fill_n_per_page(paging_spec: &pco::PagingSpec, n: usize) -> Result<Vec<usize>, PcoError> {
  let mut not_implemented = false;

  let n_per_page = match &paging_spec {
    pco::PagingSpec::EqualPagesUpTo(max_page_n) => {
      let n_pages = n.div_ceil(*max_page_n);
      let mut res = Vec::new();
      let mut start = 0;
      for i in 0..n_pages {
        let end = ((i + 1) * n) / n_pages;
        res.push(end - start);
        start = end;
      }
      res
    },
    pco::PagingSpec::Exact(n_per_page) => n_per_page.to_vec(),
    _ => {
      not_implemented = true;
      Vec::new()
    }
  };

  if not_implemented {
    return Err(PcoCompressionError);
  }

  let summed_n: usize = n_per_page.iter().sum();
  if summed_n != n {
    return Err(PcoCompressionError);
  }

  for &page_n in &n_per_page {
    if page_n == 0 {
      return Err(PcoCompressionError);
    }
  }

  Ok(n_per_page)
}

fn _simple_compress<T: NumberLike>(
  src: *const u8,
  src_len: u32,
  dst: *mut u8,
  dst_len: u32,
  level: u32,
  compressed_bytes_written: *mut u32
) -> PcoError {
  let src_slice = unsafe { std::slice::from_raw_parts(src as *const T, (src_len as usize) / std::mem::size_of::<T>()) };
  let dst_slice = unsafe { std::slice::from_raw_parts_mut(dst, dst_len as usize) };

  let config = pco::ChunkConfig::default().with_compression_level(level as usize);
  let file_compressor = pco::standalone::FileCompressor::default().with_n_hint(src_slice.len());
  let mut cursor = std::io::Cursor::new(dst_slice);

  let mut result = file_compressor.write_header(&mut cursor);
  if result.is_err() {
    return PcoCompressionError;
  }

  // here we use the paging spec to determine chunks; each chunk has 1 page

  let fill_n_per_page_result = fill_n_per_page(&config.paging_spec, src_slice.len());
  if fill_n_per_page_result.is_err() {
    return fill_n_per_page_result.unwrap_err();
  }
  let n_per_page = fill_n_per_page_result.ok().unwrap();

  let mut start = 0;
  let mut this_chunk_config = config.clone();
  for &page_n in &n_per_page {
    let end = start + page_n;
    this_chunk_config.paging_spec = pco::PagingSpec::Exact(vec![page_n]);
    let chunk_compressor =
      file_compressor.chunk_compressor(&src_slice[start..end], &this_chunk_config);

    if chunk_compressor.is_err() {
      return PcoCompressionError;
    }

    result = chunk_compressor.ok().unwrap().write_chunk(&mut cursor);
    if result.is_err() {
      return PcoCompressionError;
    }

    start = end;
  }

  result = file_compressor.write_footer(&mut cursor);
  if result.is_err() {
    return PcoCompressionError;
  }

  unsafe { *compressed_bytes_written = cursor.position() as u32 };

  PcoSuccess
}

fn _simple_decompress<T: NumberLike>(
  src: *const u8,
  src_len: u32,
  dst: *mut u8,
  dst_len: u32
) -> PcoError {
  let src_slice = unsafe { std::slice::from_raw_parts(src, src_len as usize) };
  let dst_slice = unsafe { std::slice::from_raw_parts_mut(dst as *mut T, (dst_len as usize) / std::mem::size_of::<T>()) };
  match pco::standalone::simple_decompress_into::<T>(src_slice, dst_slice) {
    Err(_) => PcoError::PcoDecompressionError,
    Ok(_) => PcoError::PcoSuccess
  }
}

fn _file_size<T: NumberLike>(
  len: u32,
  file_size: *mut u32
) -> PcoError {
  match pco::standalone::guarantee::file_size::<T::L>(len as usize, &pco::PagingSpec::EqualPagesUpTo(DEFAULT_MAX_PAGE_N)) {
    Err(_) => PcoError::PcoInvalidArgument,
    Ok(v) => {
      unsafe { *file_size = v as u32 };
      PcoError::PcoSuccess
    }
  }
}

#[no_mangle]
pub extern "C" fn pco_simple_compress(
  dtype: u8,
  src: *const u8,
  src_len: u32,
  dst: *mut u8,
  dst_len: u32,
  level: u32,
  compressed_bytes_written: *mut u32
) -> PcoError {
  let Some(dtype) = CoreDataType::from_byte(dtype) else {
    return PcoInvalidType;
  };

  match_dtype!(
    dtype,
    _simple_compress,
    (src, src_len, dst, dst_len, level, compressed_bytes_written)
  )
}

#[no_mangle]
pub extern "C" fn pco_simple_decompress(
  dtype: u8,
  src: *const u8,
  src_len: u32,
  dst: *mut u8,
  dst_len: u32
) -> PcoError {
  let Some(dtype) = CoreDataType::from_byte(dtype) else {
    return PcoInvalidType;
  };

  match_dtype!(
    dtype,
    _simple_decompress,
    (src, src_len, dst, dst_len)
  )
}

#[no_mangle]
pub extern "C" fn pco_file_size(
  dtype: u8,
  len: u32,
  file_size: *mut u32
) -> PcoError {
  let Some(dtype) = CoreDataType::from_byte(dtype) else {
    return PcoInvalidType;
  };

  match_dtype!(
    dtype,
    _file_size,
    (len, file_size)
  )
}
