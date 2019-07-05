SELECT 
    name, 
    hash_of_all_files, 
    hash_of_uncompressed_files, 
    uncompressed_hash_of_compressed_files
FROM system.parts_columns
ORDER BY name
LIMIT 50
