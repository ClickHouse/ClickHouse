-- Every data skipping index type must carry a non-empty description in its structured documentation.
SELECT name FROM system.data_skipping_index_types WHERE length(description) < 10;
