-- Every dictionary source registered in this build must carry a non-empty description in its structured documentation.
SELECT name FROM system.dictionary_sources WHERE length(description) < 10;
