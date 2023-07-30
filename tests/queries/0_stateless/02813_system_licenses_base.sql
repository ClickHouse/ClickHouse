SELECT * REPLACE substring(license_text, 1, position(license_text, '\n')) AS license_text FROM system.licenses WHERE library_name = 'poco' FORMAT Vertical;
