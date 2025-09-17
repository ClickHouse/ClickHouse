-- Tests setting 'network_compression_method'

SET network_compression_method = 'NONE';
SELECT 1, 1.0, 'abc'; -- The major classes of data types which codecs support or don't support

SET network_compression_method = 'LZ4';
SELECT 1, 1.0, 'abc';

SET network_compression_method = 'LZ4HC';
SELECT 1, 1.0, 'abc';

SET network_compression_method = 'ZSTD';
SELECT 1, 1.0, 'abc';

SET network_compression_method = 'NOT_A_CODEC';
SELECT 1, 1.0, 'abc'; -- { clientError BAD_ARGUMENTS }

-- At this point, the connection parameters are broken and cannot be repaired
-- Users at least get an error message what is wrong
SET network_compression_method = 'ZSTD'; -- { clientError BAD_ARGUMENTS }
