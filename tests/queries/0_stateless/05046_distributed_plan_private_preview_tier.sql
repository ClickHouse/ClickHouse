-- The distributed-plan settings a query needs to run on stateless workers are in the
-- PRIVATE_PREVIEW tier (not EXPERIMENTAL). Assert the exact names and tiers so a
-- regression that promotes only some of them, or leaves them experimental, is caught.

SELECT name, tier FROM system.settings
WHERE name IN (
    'make_distributed_plan',
    'distributed_plan_workers_num',
    'distributed_plan_workers_provisioning_timeout_ms',
    'distributed_plan_prefer_replicas_over_workers')
ORDER BY name;

-- None of them remain in the Experimental tier.
SELECT count() FROM system.settings
WHERE tier = 'Experimental' AND name IN (
    'make_distributed_plan',
    'distributed_plan_workers_num',
    'distributed_plan_workers_provisioning_timeout_ms',
    'distributed_plan_prefer_replicas_over_workers');
