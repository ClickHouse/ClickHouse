SELECT COLUMNS(license_text, library_name) APPLY (length) FROM system.licenses ORDER BY library_name LIMIT 1;
