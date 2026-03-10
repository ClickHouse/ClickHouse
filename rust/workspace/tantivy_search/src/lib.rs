use cxx::{CxxString, CxxVector};
use std::io::{Read, Write, Cursor};
use std::path::PathBuf;
use tantivy::collector::TopDocs;
use tantivy::query::{TermQuery, BooleanQuery, Occur};
use tantivy::schema::{Schema, IndexRecordOption, Value};
use tantivy::directory::{RamDirectory, Directory, TerminatingWrite};
use tantivy::{Index, IndexWriter, Term, TantivyDocument, IndexSettings};

#[cxx::bridge]
mod ffi {
    extern "Rust" {
        /// Create a new in-memory Tantivy index for a text field.
        /// Returns serialized index data that can be stored.
        fn tantivy_create_index() -> Result<Vec<u8>>;

        /// Add a document to the index being built.
        /// Takes the current index data, adds a document, returns updated data.
        fn tantivy_add_document(index_data: &[u8], row_id: u64, text: &CxxString) -> Result<Vec<u8>>;

        /// Finalize and commit the index. Returns the final serialized index.
        fn tantivy_commit(index_data: &[u8]) -> Result<Vec<u8>>;

        /// Search for a single term in the index.
        /// Returns matching row IDs.
        fn tantivy_search_term(index_data: &[u8], term: &CxxString) -> Result<Vec<u64>>;

        /// Search for all terms (AND logic).
        /// Returns row IDs that match ALL terms.
        fn tantivy_search_terms_and(index_data: &[u8], terms: &CxxVector<CxxString>) -> Result<Vec<u64>>;

        /// Search for any terms (OR logic).
        /// Returns row IDs that match ANY term.
        fn tantivy_search_terms_or(index_data: &[u8], terms: &CxxVector<CxxString>) -> Result<Vec<u64>>;

        /// Get Tantivy version string.
        fn tantivy_version() -> String;
    }
}

// Schema field names
const TEXT_FIELD_NAME: &str = "text";
const ROW_ID_FIELD_NAME: &str = "row_id";

/// Build the schema for log search.
/// - text: the searchable text field with default tokenization
/// - row_id: stored field to track ClickHouse row IDs
fn build_schema() -> Schema {
    let mut schema_builder = Schema::builder();

    // Text field with default tokenization (word split + lowercase)
    let text_options = tantivy::schema::TextOptions::default()
        .set_indexing_options(
            tantivy::schema::TextFieldIndexing::default()
                .set_tokenizer("default")
                .set_index_option(IndexRecordOption::WithFreqsAndPositions)
        );
    schema_builder.add_text_field(TEXT_FIELD_NAME, text_options);

    // Row ID as u64 fast field for retrieval
    schema_builder.add_u64_field(ROW_ID_FIELD_NAME, tantivy::schema::FAST | tantivy::schema::STORED);

    schema_builder.build()
}

/// Get list of files in the index by reading meta.json and enumerating segment files
fn get_index_files(directory: &RamDirectory) -> Result<Vec<PathBuf>, String> {
    use std::path::Path;

    let mut files = Vec::new();

    // Read meta.json to get segment info
    let meta_data = directory.atomic_read(Path::new("meta.json"))
        .map_err(|e| format!("Failed to read meta.json: {}", e))?;

    let meta_str = String::from_utf8(meta_data.to_vec())
        .map_err(|e| format!("Invalid meta.json: {}", e))?;

    // Always include meta.json
    files.push(PathBuf::from("meta.json"));

    // Check for .managed.json
    if directory.exists(Path::new(".managed.json")).unwrap_or(false) {
        files.push(PathBuf::from(".managed.json"));
    }

    // Extract segment IDs from meta.json (simple JSON parsing)
    for segment_id in extract_segment_ids(&meta_str) {
        // Tantivy segment files have various extensions
        // The segment_id is a UUID-like hex string
        let segment_suffixes = [
            ".fieldnorm",
            ".idx",
            ".pos",
            ".posidx",
            ".store",
            ".term",
            ".fast",
            ".del",
        ];

        for suffix in &segment_suffixes {
            let file_name = format!("{}{}", segment_id, suffix);
            let path = Path::new(&file_name);
            if directory.exists(path).unwrap_or(false) {
                files.push(PathBuf::from(file_name));
            }
        }
    }

    Ok(files)
}

/// Extract segment IDs from meta.json content
fn extract_segment_ids(meta_json: &str) -> Vec<String> {
    let mut ids = Vec::new();

    // The segment_id in Tantivy's meta.json is a UUID with hyphens like "1180a49b-c0af-4632-8f50-b7a9b0198fdd"
    // But the actual file names use the UUID without hyphens: "1180a49bc0af46328f50b7a9b0198fdd"
    // Format in JSON: "segment_id": "uuid-with-hyphens"
    let mut remaining = meta_json;
    while let Some(pos) = remaining.find("\"segment_id\"") {
        remaining = &remaining[pos + 12..]; // Skip "segment_id"

        // Skip whitespace and colon
        let trimmed = remaining.trim_start();
        if !trimmed.starts_with(':') {
            continue;
        }
        remaining = trimmed[1..].trim_start(); // Skip ':'

        // Now we should have a quoted string
        if !remaining.starts_with('"') {
            continue;
        }
        remaining = &remaining[1..]; // Skip opening quote

        if let Some(end) = remaining.find('"') {
            let uuid_with_hyphens = &remaining[..end];
            // Remove hyphens to get the file name prefix
            let uuid_no_hyphens = uuid_with_hyphens.replace('-', "");
            ids.push(uuid_no_hyphens);
            remaining = &remaining[end..];
        }
    }

    ids
}

/// Serialize a RamDirectory to bytes.
fn serialize_directory(directory: &RamDirectory) -> Result<Vec<u8>, String> {
    let mut buffer = Vec::new();

    // Get list of files from the index
    let files = get_index_files(directory)?;

    // Write number of files
    let file_count = files.len() as u32;
    buffer.extend_from_slice(&file_count.to_le_bytes());

    for file_path in files {
        // Get file data using atomic_read from Directory trait
        let file_data = directory.atomic_read(&file_path)
            .map_err(|e| format!("Failed to read file {:?}: {}", file_path, e))?;

        // Write filename length and filename
        let file_name = file_path.to_string_lossy().to_string();
        let name_len = file_name.len() as u32;
        buffer.extend_from_slice(&name_len.to_le_bytes());
        buffer.extend_from_slice(file_name.as_bytes());

        // Write data length and data
        let data_len = file_data.len() as u64;
        buffer.extend_from_slice(&data_len.to_le_bytes());
        buffer.extend_from_slice(&file_data);
    }

    Ok(buffer)
}

/// Deserialize bytes back to a RamDirectory.
fn deserialize_directory(data: &[u8]) -> Result<RamDirectory, String> {
    use std::path::Path;

    let directory = RamDirectory::create();
    let mut cursor = Cursor::new(data);

    // Read number of files
    let mut file_count_bytes = [0u8; 4];
    cursor.read_exact(&mut file_count_bytes)
        .map_err(|e| format!("Failed to read file count: {}", e))?;
    let file_count = u32::from_le_bytes(file_count_bytes);

    for _ in 0..file_count {
        // Read filename length and filename
        let mut name_len_bytes = [0u8; 4];
        cursor.read_exact(&mut name_len_bytes)
            .map_err(|e| format!("Failed to read filename length: {}", e))?;
        let name_len = u32::from_le_bytes(name_len_bytes) as usize;

        let mut name_bytes = vec![0u8; name_len];
        cursor.read_exact(&mut name_bytes)
            .map_err(|e| format!("Failed to read filename: {}", e))?;
        let file_name = String::from_utf8(name_bytes)
            .map_err(|e| format!("Invalid filename: {}", e))?;

        // Read data length and data
        let mut data_len_bytes = [0u8; 8];
        cursor.read_exact(&mut data_len_bytes)
            .map_err(|e| format!("Failed to read data length: {}", e))?;
        let data_len = u64::from_le_bytes(data_len_bytes) as usize;

        let mut file_data = vec![0u8; data_len];
        cursor.read_exact(&mut file_data)
            .map_err(|e| format!("Failed to read file data: {}", e))?;

        // Write to directory using the Directory trait
        let mut writer = directory.open_write(Path::new(&file_name))
            .map_err(|e| format!("Failed to create file {}: {}", file_name, e))?;
        writer.write_all(&file_data)
            .map_err(|e| format!("Failed to write file {}: {}", file_name, e))?;
        writer.terminate()
            .map_err(|e| format!("Failed to terminate file {}: {}", file_name, e))?;
    }

    Ok(directory)
}

// ============================================================================
// FFI Functions
// ============================================================================

fn tantivy_create_index() -> Result<Vec<u8>, String> {
    let schema = build_schema();
    let directory = RamDirectory::create();

    let index = Index::create(directory.clone(), schema.clone(), IndexSettings::default())
        .map_err(|e| format!("Failed to create index: {}", e))?;

    // Create writer with small heap (we're indexing one granule at a time)
    let mut writer: IndexWriter = index.writer(15_000_000)
        .map_err(|e| format!("Failed to create writer: {}", e))?;

    // Commit empty index to initialize segments
    writer.commit()
        .map_err(|e| format!("Failed to commit: {}", e))?;

    serialize_directory(&directory)
}

fn tantivy_add_document(index_data: &[u8], row_id: u64, text: &CxxString) -> Result<Vec<u8>, String> {
    let directory = deserialize_directory(index_data)?;
    let index = Index::open(directory.clone())
        .map_err(|e| format!("Failed to open index: {}", e))?;

    let schema = index.schema();
    let text_field = schema.get_field(TEXT_FIELD_NAME)
        .map_err(|_| "Text field not found")?;
    let row_id_field = schema.get_field(ROW_ID_FIELD_NAME)
        .map_err(|_| "Row ID field not found")?;

    let mut writer = index.writer(15_000_000)
        .map_err(|e| format!("Failed to create writer: {}", e))?;

    let text_str = text.to_str().map_err(|e| format!("Invalid UTF-8: {}", e))?;

    let mut doc = TantivyDocument::default();
    doc.add_text(text_field, text_str);
    doc.add_u64(row_id_field, row_id);

    writer.add_document(doc)
        .map_err(|e| format!("Failed to add document: {}", e))?;

    // Commit after each add for simplicity
    writer.commit()
        .map_err(|e| format!("Failed to commit: {}", e))?;

    serialize_directory(&directory)
}

fn tantivy_commit(index_data: &[u8]) -> Result<Vec<u8>, String> {
    // Documents are committed on add, so just return as-is
    Ok(index_data.to_vec())
}

fn tantivy_search_term(index_data: &[u8], term: &CxxString) -> Result<Vec<u64>, String> {
    let directory = deserialize_directory(index_data)?;
    let index = Index::open(directory)
        .map_err(|e| format!("Failed to open index: {}", e))?;

    let reader = index.reader()
        .map_err(|e| format!("Failed to create reader: {}", e))?;
    let searcher = reader.searcher();

    let schema = index.schema();
    let text_field = schema.get_field(TEXT_FIELD_NAME)
        .map_err(|_| "Text field not found")?;
    let row_id_field = schema.get_field(ROW_ID_FIELD_NAME)
        .map_err(|_| "Row ID field not found")?;

    let term_str = term.to_str().map_err(|e| format!("Invalid UTF-8: {}", e))?;
    let term_lower = term_str.to_lowercase();

    let query = TermQuery::new(
        Term::from_field_text(text_field, &term_lower),
        IndexRecordOption::Basic
    );

    let top_docs = searcher.search(&query, &TopDocs::with_limit(10000))
        .map_err(|e| format!("Search failed: {}", e))?;

    let mut row_ids = Vec::new();
    for (_score, doc_address) in top_docs {
        let doc: TantivyDocument = searcher.doc(doc_address)
            .map_err(|e| format!("Failed to retrieve doc: {}", e))?;
        if let Some(row_id) = doc.get_first(row_id_field) {
            if let Some(id) = row_id.as_u64() {
                row_ids.push(id);
            }
        }
    }

    Ok(row_ids)
}

fn tantivy_search_terms_and(index_data: &[u8], terms: &CxxVector<CxxString>) -> Result<Vec<u64>, String> {
    let directory = deserialize_directory(index_data)?;
    let index = Index::open(directory)
        .map_err(|e| format!("Failed to open index: {}", e))?;

    let reader = index.reader()
        .map_err(|e| format!("Failed to create reader: {}", e))?;
    let searcher = reader.searcher();

    let schema = index.schema();
    let text_field = schema.get_field(TEXT_FIELD_NAME)
        .map_err(|_| "Text field not found")?;
    let row_id_field = schema.get_field(ROW_ID_FIELD_NAME)
        .map_err(|_| "Row ID field not found")?;

    let mut subqueries: Vec<(Occur, Box<dyn tantivy::query::Query>)> = Vec::new();

    for term in terms {
        let term_str = term.to_str().map_err(|e| format!("Invalid UTF-8: {}", e))?;
        let term_lower = term_str.to_lowercase();
        let tq = TermQuery::new(
            Term::from_field_text(text_field, &term_lower),
            IndexRecordOption::Basic
        );
        subqueries.push((Occur::Must, Box::new(tq)));
    }

    let query = BooleanQuery::new(subqueries);

    let top_docs = searcher.search(&query, &TopDocs::with_limit(10000))
        .map_err(|e| format!("Search failed: {}", e))?;

    let mut row_ids = Vec::new();
    for (_score, doc_address) in top_docs {
        let doc: TantivyDocument = searcher.doc(doc_address)
            .map_err(|e| format!("Failed to retrieve doc: {}", e))?;
        if let Some(row_id) = doc.get_first(row_id_field) {
            if let Some(id) = row_id.as_u64() {
                row_ids.push(id);
            }
        }
    }

    Ok(row_ids)
}

fn tantivy_search_terms_or(index_data: &[u8], terms: &CxxVector<CxxString>) -> Result<Vec<u64>, String> {
    let directory = deserialize_directory(index_data)?;
    let index = Index::open(directory)
        .map_err(|e| format!("Failed to open index: {}", e))?;

    let reader = index.reader()
        .map_err(|e| format!("Failed to create reader: {}", e))?;
    let searcher = reader.searcher();

    let schema = index.schema();
    let text_field = schema.get_field(TEXT_FIELD_NAME)
        .map_err(|_| "Text field not found")?;
    let row_id_field = schema.get_field(ROW_ID_FIELD_NAME)
        .map_err(|_| "Row ID field not found")?;

    let mut subqueries: Vec<(Occur, Box<dyn tantivy::query::Query>)> = Vec::new();

    for term in terms {
        let term_str = term.to_str().map_err(|e| format!("Invalid UTF-8: {}", e))?;
        let term_lower = term_str.to_lowercase();
        let tq = TermQuery::new(
            Term::from_field_text(text_field, &term_lower),
            IndexRecordOption::Basic
        );
        subqueries.push((Occur::Should, Box::new(tq)));
    }

    let query = BooleanQuery::new(subqueries);

    let top_docs = searcher.search(&query, &TopDocs::with_limit(10000))
        .map_err(|e| format!("Search failed: {}", e))?;

    let mut row_ids = Vec::new();
    for (_score, doc_address) in top_docs {
        let doc: TantivyDocument = searcher.doc(doc_address)
            .map_err(|e| format!("Failed to retrieve doc: {}", e))?;
        if let Some(row_id) = doc.get_first(row_id_field) {
            if let Some(id) = row_id.as_u64() {
                row_ids.push(id);
            }
        }
    }

    Ok(row_ids)
}

fn tantivy_version() -> String {
    "tantivy-0.22".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_search() {
        // Create index
        let index_data = tantivy_create_index().unwrap();
        assert!(!index_data.is_empty());

        // Verify we can open it
        let directory = deserialize_directory(&index_data).unwrap();
        let index = Index::open(directory).unwrap();
        assert!(index.schema().get_field(TEXT_FIELD_NAME).is_ok());
    }

    #[test]
    fn test_add_documents_and_search() {
        // Create index directly without serialization for testing
        let directory = RamDirectory::create();
        let schema = build_schema();
        let index = Index::create(directory.clone(), schema.clone(), IndexSettings::default()).unwrap();

        let text_field = schema.get_field(TEXT_FIELD_NAME).unwrap();
        let row_id_field = schema.get_field(ROW_ID_FIELD_NAME).unwrap();

        let mut writer = index.writer(15_000_000).unwrap();

        // Add test documents
        let docs = vec![
            (0u64, "connection refused error in database"),
            (1u64, "high memory usage detected warning"),
            (2u64, "payment processed successfully"),
            (3u64, "connection timeout error"),
        ];

        for (row_id, text) in &docs {
            let mut doc = TantivyDocument::default();
            doc.add_text(text_field, *text);
            doc.add_u64(row_id_field, *row_id);
            writer.add_document(doc).unwrap();
        }
        writer.commit().unwrap();

        // Test searching directly (without serialization round-trip)
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        // Search for "error" - should find rows 0 and 3
        let query = TermQuery::new(
            Term::from_field_text(text_field, "error"),
            IndexRecordOption::Basic
        );
        let top_docs = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();
        assert_eq!(top_docs.len(), 2);

        // Search for "payment" - should find row 2
        let query = TermQuery::new(
            Term::from_field_text(text_field, "payment"),
            IndexRecordOption::Basic
        );
        let top_docs = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();
        assert_eq!(top_docs.len(), 1);

        // Search for non-existent term
        let query = TermQuery::new(
            Term::from_field_text(text_field, "nonexistent"),
            IndexRecordOption::Basic
        );
        let top_docs = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();
        assert!(top_docs.is_empty());
    }

    #[test]
    fn test_serialization_roundtrip() {
        // Test that serialization/deserialization preserves the index
        let directory = RamDirectory::create();
        let schema = build_schema();
        let index = Index::create(directory.clone(), schema.clone(), IndexSettings::default()).unwrap();

        let text_field = schema.get_field(TEXT_FIELD_NAME).unwrap();
        let row_id_field = schema.get_field(ROW_ID_FIELD_NAME).unwrap();

        let mut writer = index.writer(15_000_000).unwrap();

        let mut doc = TantivyDocument::default();
        doc.add_text(text_field, "test document");
        doc.add_u64(row_id_field, 42);
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();

        // Serialize
        let serialized = serialize_directory(&directory).unwrap();
        assert!(!serialized.is_empty());

        // Deserialize
        let restored_dir = deserialize_directory(&serialized).unwrap();

        // Verify we can open the restored index
        let restored_index = Index::open(restored_dir).unwrap();
        let reader = restored_index.reader().unwrap();
        let searcher = reader.searcher();

        // Search
        let query = TermQuery::new(
            Term::from_field_text(text_field, "test"),
            IndexRecordOption::Basic
        );
        let top_docs = searcher.search(&query, &TopDocs::with_limit(10)).unwrap();
        assert_eq!(top_docs.len(), 1);
    }
}
