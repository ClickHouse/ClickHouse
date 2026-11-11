-- Every format registered in this build must carry a non-empty description in its structured documentation.
SELECT name FROM system.formats WHERE length(description) < 10;
