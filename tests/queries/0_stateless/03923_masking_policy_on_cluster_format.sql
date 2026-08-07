-- ON CLUSTER used to be formatted before ON table, which made the output
-- unparseable and crashed the server in debug builds.
-- Instead, the expected outcome of the below query should be SUPPORT_IS_DISABLED and not a LOGICAL_ERROR.
CREATE MASKING POLICY p ON db.t ON CLUSTER mycluster UPDATE email = '***' TO ALL; -- { serverError SUPPORT_IS_DISABLED }
