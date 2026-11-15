-- { echo }

SET compatibility = '24.8';

SELECT value, changed FROM system.settings WHERE name = 'output_format_json_quote_64bit_integers';

SELECT count()
FROM system.settings
WHERE changed AND is_obsolete AND name IN
(
    'allow_experimental_bfloat16_type',
    'allow_experimental_refreshable_materialized_view',
    'allow_experimental_vector_similarity_index',
    'enable_vector_similarity_index'
);

SELECT count()
FROM system.warnings
WHERE message LIKE '%Obsolete setting%'
    AND (
        message LIKE '%allow_experimental_bfloat16_type%'
        OR message LIKE '%allow_experimental_refreshable_materialized_view%'
        OR message LIKE '%allow_experimental_vector_similarity_index%'
        OR message LIKE '%enable_vector_similarity_index%'
    );
