-- Every compression codec must carry a non-empty description in its structured documentation.
SELECT name FROM system.codecs WHERE length(description) < 10;
