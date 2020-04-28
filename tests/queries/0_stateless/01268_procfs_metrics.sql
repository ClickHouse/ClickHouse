# Sandbox does not provide CAP_NET_ADMIN capability but does have ProcFS mounted at /proc
# This ensures that OS metrics can be collected

select count() > 0 from system.query_log where type = 2 settings log_queries = 1 Format Null;
select count() > 0 from system.query_log where type = 2 and has(ProfileEvents.Names, 'OSReadChars');
