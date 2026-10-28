-- The sorting code of sort_by_label() is ported from Go and facette/natsort, so system.licenses lists their licenses.
SELECT * REPLACE substring(license_text, 1, position(license_text, '\n')) AS license_text FROM system.licenses WHERE library_name IN ('golang', 'natsort') ORDER BY library_name FORMAT Vertical;
