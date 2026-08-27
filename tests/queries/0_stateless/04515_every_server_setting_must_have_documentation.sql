-- Every server setting must carry a non-empty description in its structured documentation.
SELECT name FROM system.server_settings WHERE length(description) < 10;
