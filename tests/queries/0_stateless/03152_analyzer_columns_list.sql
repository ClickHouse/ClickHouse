SELECT COLUMNS(license_text, library_name) APPLY (length) FROM system.licenses ORDER BY library_name LIMIT 1;

SELECT COLUMNS(license_text, library_name, xyz) APPLY (length) FROM system.licenses; -- { serverError UNKNOWN_IDENTIFIER }
