-- The system.masking_policies table and introspection are present in open-source builds, even though
-- masking policies can only be created and applied in ClickHouse Cloud. So SHOW MASKING POLICIES returns
-- an empty result (instead of throwing), and the table is queryable and empty. Creating, altering and
-- dropping masking policies remains cloud-only.

SHOW MASKING POLICIES;
SELECT count() FROM system.masking_policies;
SELECT name, short_name, database, table, expression, where_condition, priority, apply_to_all FROM system.masking_policies;

CREATE MASKING POLICY mp_04405 ON db.t UPDATE x = 'masked' TO ALL; -- { serverError SUPPORT_IS_DISABLED }
