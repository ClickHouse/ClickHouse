-- Merge tree setting 'index_granularity' is unconditionally read-only (changing it would mean rewriting all parts).
-- There was a bug that the system table wouldn't show it as read-only.
SELECT name, readonly FROM system.merge_tree_settings WHERE name = 'index_granularity';
