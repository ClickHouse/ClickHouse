-- Tags: no-fasttest
-- Smoke test: reading from system.certificates exercises X509Certificate loading.
SELECT count() > 0 FROM system.certificates;
