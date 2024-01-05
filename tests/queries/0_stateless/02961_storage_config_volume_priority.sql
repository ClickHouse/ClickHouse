SELECT
    volume_name,
    volume_priority
FROM system.storage_policies
WHERE policy_name = 'policy_02961'
ORDER BY volume_priority ASC;

