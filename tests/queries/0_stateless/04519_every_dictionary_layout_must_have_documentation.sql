-- Every dictionary layout must carry a non-empty description in its structured documentation.
SELECT name FROM system.dictionary_layouts WHERE length(description) < 10;
