-- Every disk type must carry a non-empty description in its structured documentation.
SELECT name FROM system.disk_types WHERE length(description) < 10;
