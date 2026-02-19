SELECT
    changeable_without_restart
FROM system.server_settings
WHERE name = 'allow_nullable_tuple_in_extracted_subcolumns';
