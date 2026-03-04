-- Test that SYSTEM WAIT BLOBS CLEANUP parses correctly
EXPLAIN SYNTAX SYSTEM WAIT BLOBS CLEANUP 'default';

-- Test that it throws for a non-object-storage disk
SYSTEM WAIT BLOBS CLEANUP 'default'; -- { serverError BAD_ARGUMENTS }
