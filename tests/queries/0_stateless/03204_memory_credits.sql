SET allow_experimental_features = 1;
SYSTEM RELOAD CONFIG;
SELECT ProfileEvents['MemoryCredits'] FROM system.metrics;
