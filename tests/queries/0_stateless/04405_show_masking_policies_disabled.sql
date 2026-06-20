-- Masking policies are available only in ClickHouse Cloud; on OSS builds SHOW MASKING POLICIES must
-- report a clear SUPPORT_IS_DISABLED error instead of a confusing UNKNOWN_TABLE error about
-- system.masking_policies (which does not exist in OSS).
SHOW MASKING POLICIES; -- { serverError SUPPORT_IS_DISABLED }
