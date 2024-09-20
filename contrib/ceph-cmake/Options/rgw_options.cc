#include "common/options.h"


std::vector<Option> get_rgw_options() {
  return std::vector<Option>({
    Option("rgw_acl_grants_max_num", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("The maximum number of ACL grants in a single request.")
    .set_default(100)
    .add_service("rgw"),

    Option("rgw_user_policies_max_num", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("The maximum number of IAM user policies for a single user.")
    .set_default(100)
    .add_service("rgw"),

    Option("rgw_cors_rules_max_num", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("The maximum number of CORS rules in a single request.")
    .set_default(100)
    .add_service("rgw"),

    Option("rgw_delete_multi_obj_max_num", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("The maximum number of objects in a single multi-object delete request.")
    .set_default(1000)
    .add_service("rgw"),

    Option("rgw_website_routing_rules_max_num", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("The maximum number of website routing rules in a single request.")
    .set_default(50)
    .add_service("rgw"),

    Option("rgw_rados_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enables LTTng-UST tracepoints.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_op_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enables LTTng-UST operator tracepoints.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_max_chunk_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("The maximum RGW chunk size.")
    .set_long_description("The chunk size is the size of RADOS I/O requests that RGW sends when accessing data objects. RGW read and write operations will never request more than this amount in a single request. This also defines the RGW head object size, as head operations need to be atomic, and anything larger than this would require more than a single operation. When RGW objects are written to the default storage class, up to this amount of payload data will be stored alongside metadata in the head object.")
    .set_default(4_M)
    .add_service("rgw"),

    Option("rgw_put_obj_min_window_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("The minimum RADOS write window size (in bytes).")
    .set_long_description("The window size determines the total concurrent RADOS writes of a single RGW object. When writing an object RGW will send multiple chunks to RADOS. The total size of the writes does not exceed the window size. The window size may be adjusted dynamically in order to better utilize the pipe.")
    .set_default(16_M)
    .add_service("rgw")
    .add_see_also({"rgw_put_obj_max_window_size", "rgw_max_chunk_size"}),

    Option("rgw_put_obj_max_window_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("The maximum RADOS write window size (in bytes).")
    .set_long_description("The window size may be dynamically adjusted, but will not surpass this value.")
    .set_default(64_M)
    .add_service("rgw")
    .add_see_also({"rgw_put_obj_min_window_size", "rgw_max_chunk_size"}),

    Option("rgw_max_put_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("The maximum size (in bytes) of regular (non multi-part) object upload.")
    .set_long_description("Plain object upload is capped at this amount of data. In order to upload larger objects, a special upload mechanism is required. The S3 API provides the multi-part upload, and Swift provides DLO and SLO.")
    .set_default(5_G)
    .add_service("rgw"),

    Option("rgw_max_put_param_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("The maximum size (in bytes) of data input of certain RESTful requests.")
    .set_default(1_M)
    .add_service("rgw"),

    Option("rgw_max_attr_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("The maximum length of metadata value. 0 skips the check")
    .set_default(0)
    .add_service("rgw"),

    Option("rgw_max_attr_name_len", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("The maximum length of metadata name. 0 skips the check")
    .set_default(0)
    .add_service("rgw"),

    Option("rgw_max_attrs_num_in_req", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("The maximum number of metadata items that can be put via single request")
    .set_default(0)
    .add_service("rgw"),

    Option("rgw_override_bucket_index_max_shards", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("The default number of bucket index shards for newly-created buckets. This value overrides bucket_index_max_shards stored in the zone. Setting this value in the zone is preferred, because it applies globally to all radosgw daemons running in the zone.")
    .set_default(0)
    .add_service("rgw"),

    Option("rgw_bucket_index_max_aio", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Max number of concurrent RADOS requests when handling bucket shards.")
    .set_default(128)
    .add_service("rgw"),

    Option("rgw_multi_obj_del_max_aio", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Max number of concurrent RADOS requests per multi-object delete request.")
    .set_default(16)
    .add_service("rgw"),

    Option("rgw_enable_quota_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enables the quota maintenance thread.")
    .set_long_description("The quota maintenance thread is responsible for quota related maintenance work. The thread itself can be disabled, but in order for quota to work correctly, at least one RGW in each zone needs to have this thread running. Having the thread enabled on multiple RGW processes within the same zone can spread some of the maintenance work between them.")
    .set_default(true)
    .add_service("rgw")
    .add_see_also({"rgw_enable_gc_threads", "rgw_enable_lc_threads"}),

    Option("rgw_enable_gc_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enables the garbage collection maintenance thread.")
    .set_long_description("The garbage collection maintenance thread is responsible for garbage collector maintenance work. The thread itself can be disabled, but in order for garbage collection to work correctly, at least one RGW in each zone needs to have this thread running.  Having the thread enabled on multiple RGW processes within the same zone can spread some of the maintenance work between them.")
    .set_default(true)
    .add_service("rgw")
    .add_see_also({"rgw_enable_quota_threads", "rgw_enable_lc_threads"}),

    Option("rgw_enable_lc_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enables the lifecycle maintenance thread. This is required on at least one rgw for each zone.")
    .set_long_description("The lifecycle maintenance thread is responsible for lifecycle related maintenance work. The thread itself can be disabled, but in order for lifecycle to work correctly, at least one RGW in each zone needs to have this thread running. Havingthe thread enabled on multiple RGW processes within the same zone can spread some of the maintenance work between them.")
    .set_default(true)
    .add_service("rgw")
    .add_see_also({"rgw_enable_gc_threads", "rgw_enable_quota_threads"}),

    Option("rgw_data", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Alternative location for RGW configuration.")
    .set_long_description("If this is set, the different Ceph system configurables (such as the keyring file will be located in the path that is specified here.")
    .set_default("/var/lib/ceph/radosgw/$cluster-$id")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .add_service("rgw"),

    Option("rgw_enable_apis", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("A list of set of RESTful APIs that rgw handles.")
    .set_default("s3, s3website, swift, swift_auth, admin, sts, iam, notifications")
    .add_service("rgw"),

    Option("rgw_cache_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable RGW metadata cache.")
    .set_long_description("The metadata cache holds metadata entries that RGW requires for processing requests. Metadata entries can be user info, bucket info, and bucket instance info. If not found in the cache, entries will be fetched from the backing RADOS store.")
    .set_default(true)
    .add_service("rgw")
    .add_see_also({"rgw_cache_lru_size"}),

    Option("rgw_cache_lru_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Max number of items in RGW metadata cache.")
    .set_long_description("When full, the RGW metadata cache evicts least recently used entries.")
    .set_default(10000)
    .add_service("rgw")
    .add_see_also({"rgw_cache_enabled"}),

    Option("rgw_dns_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("The host names that RGW uses.")
    .set_long_description("A comma separated list of DNS names. This is Needed for virtual hosting of buckets to work properly, unless configured via zonegroup configuration.")
    .add_service("rgw"),

    Option("rgw_dns_s3website_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("The host name that RGW uses for static websites (S3)")
    .set_long_description("This is needed for virtual hosting of buckets, unless configured via zonegroup configuration.")
    .add_service("rgw"),

    Option("rgw_numa_node", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("set rgw's cpu affinity to a numa node (-1 for none)")
    .set_default(-1)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("rgw"),

    Option("rgw_service_provider_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Service provider name which is contained in http response headers")
    .set_long_description("As S3 or other cloud storage providers do, http response headers should contain the name of the provider. This name will be placed in http header 'Server'.")
    .add_service("rgw"),

    Option("rgw_content_length_compat", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Multiple content length headers compatibility")
    .set_long_description("Try to handle requests with abiguous multiple content length headers (Content-Length, Http-Content-Length).")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_relaxed_region_enforcement", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Disable region constraint enforcement")
    .set_long_description("Enable requests such as bucket creation to succeed irrespective of region restrictions (Jewel compat).")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_lifecycle_work_time", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Lifecycle allowed work time")
    .set_long_description("Local time window in which the lifecycle maintenance thread can work.")
    .set_default("00:00-06:00")
    .add_service("rgw"),

    Option("rgw_lc_lock_max_time", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(90)
    .add_service("rgw"),

    Option("rgw_lc_thread_delay", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Delay after processing of bucket listing chunks (i.e., per 1000 entries) in milliseconds")
    .set_default(0)
    .add_service("rgw"),

    Option("rgw_lc_max_worker", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Number of LCWorker tasks that will be run in parallel")
    .set_long_description("Number of LCWorker tasks that will run in parallel--used to permit >1 bucket/index shards to be processed simultaneously")
    .set_default(3)
    .add_service("rgw"),

    Option("rgw_lc_max_wp_worker", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Number of workpool threads per LCWorker")
    .set_long_description("Number of threads in per-LCWorker workpools--used to accelerate per-bucket processing")
    .set_default(3)
    .add_service("rgw"),

    Option("rgw_lc_max_objs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Number of lifecycle data shards")
    .set_long_description("Number of RADOS objects to use for storing lifecycle index. This affects concurrency of lifecycle maintenance, as shards can be processed in parallel.")
    .set_default(32)
    .add_service("rgw"),

    Option("rgw_lc_max_rules", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Max number of lifecycle rules set on one bucket")
    .set_long_description("Number of lifecycle rules set on one bucket should be limited.")
    .set_default(1000)
    .add_service("rgw"),

    Option("rgw_lc_debug_interval", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("The number of seconds that simulate one \"day\" in order to debug RGW LifeCycle. Do *not* modify for a production cluster.")
    .set_long_description("For debugging RGW LifeCycle, the number of seconds that are equivalent to one simulated \"day\". Values less than 1 are ignored and do not change LifeCycle behavior. For example, during debugging if one wanted every 10 minutes to be equivalent to one day, then this would be set to 600, the number of seconds in 10 minutes.")
    .set_default(-1)
    .add_service("rgw"),

    Option("rgw_mp_lock_max_time", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Multipart upload max completion time")
    .set_long_description("Time length to allow completion of a multipart upload operation. This is done to prevent concurrent completions on the same object with the same upload id.")
    .set_default(10_min)
    .add_service("rgw"),

    Option("rgw_script_uri", Option::TYPE_STR, Option::LEVEL_DEV)
    .add_service("rgw"),

    Option("rgw_request_uri", Option::TYPE_STR, Option::LEVEL_DEV)
    .add_service("rgw"),

    Option("rgw_ignore_get_invalid_range", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Treat invalid (e.g., negative) range request as full")
    .set_long_description("Treat invalid (e.g., negative) range request as request for the full object (AWS compatibility)")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_swift_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Swift-auth storage URL")
    .set_long_description("Used in conjunction with rgw internal swift authentication. This affects the X-Storage-Url response header value.")
    .add_service("rgw")
    .add_see_also({"rgw_swift_auth_entry"}),

    Option("rgw_swift_url_prefix", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Swift URL prefix")
    .set_long_description("The URL path prefix for swift requests.")
    .set_default("swift")
    .add_service("rgw"),

    Option("rgw_swift_auth_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Swift auth URL")
    .set_long_description("Default url to which RGW connects and verifies tokens for v1 auth (if not using internal swift auth).")
    .add_service("rgw"),

    Option("rgw_swift_auth_entry", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Swift auth URL prefix")
    .set_long_description("URL path prefix for internal swift auth requests.")
    .set_default("auth")
    .add_service("rgw")
    .add_see_also({"rgw_swift_url"}),

    Option("rgw_swift_tenant_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Swift tenant name")
    .set_long_description("Tenant name that is used when constructing the swift path.")
    .add_service("rgw")
    .add_see_also({"rgw_swift_account_in_url"}),

    Option("rgw_swift_account_in_url", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Swift account encoded in URL")
    .set_long_description("Whether the swift account is encoded in the uri path (AUTH_<account>).")
    .set_default(false)
    .add_service("rgw")
    .add_see_also({"rgw_swift_tenant_name"}),

    Option("rgw_swift_enforce_content_length", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Send content length when listing containers (Swift)")
    .set_long_description("Whether content length header is needed when listing containers. When this is set to false, RGW will send extra info for each entry in the response.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_keystone_url", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("The URL to the Keystone server.")
    .add_service("rgw"),

    Option("rgw_keystone_admin_token", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("DEPRECATED: The admin token (shared secret) that is used for the Keystone requests.")
    .add_service("rgw"),

    Option("rgw_keystone_admin_token_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Path to a file containing the admin token (shared secret) that is used for the Keystone requests.")
    .add_service("rgw"),

    Option("rgw_keystone_admin_user", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Keystone admin user.")
    .add_service("rgw"),

    Option("rgw_keystone_admin_password", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("DEPRECATED: Keystone admin password.")
    .add_service("rgw"),

    Option("rgw_keystone_admin_password_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Path to a file containing the Keystone admin password.")
    .add_service("rgw"),

    Option("rgw_keystone_admin_tenant", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Keystone admin user tenant.")
    .add_service("rgw"),

    Option("rgw_keystone_admin_project", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Keystone admin user project (for Keystone v3).")
    .add_service("rgw"),

    Option("rgw_keystone_admin_domain", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Keystone admin user domain (for Keystone v3).")
    .add_service("rgw"),

    Option("rgw_keystone_service_token_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Service tokens allowing the usage of expired Keystone auth tokens")
    .set_default(false)
    .add_service("rgw")
    .add_see_also({"rgw_keystone_service_token_accepted_roles", "rgw_keystone_expired_token_cache_expiration"}),

    Option("rgw_keystone_service_token_accepted_roles", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Only users with one of these roles will be valid for service users.")
    .set_default("admin")
    .add_service("rgw")
    .add_see_also({"rgw_keystone_service_token_enabled"}),

    Option("rgw_keystone_expired_token_cache_expiration", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("The number of seconds to add to current time for expired token expiration")
    .set_default(3600)
    .add_service("rgw")
    .add_see_also({"rgw_keystone_service_token_enabled"}),

    Option("rgw_keystone_barbican_user", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Keystone user to access barbican secrets.")
    .add_service("rgw"),

    Option("rgw_keystone_barbican_password", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Keystone password for barbican user.")
    .add_service("rgw"),

    Option("rgw_keystone_barbican_tenant", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Keystone barbican user tenant (Keystone v2.0).")
    .add_service("rgw"),

    Option("rgw_keystone_barbican_project", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Keystone barbican user project (Keystone v3).")
    .add_service("rgw"),

    Option("rgw_keystone_barbican_domain", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Keystone barbican user domain.")
    .add_service("rgw"),

    Option("rgw_keystone_api_version", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Version of Keystone API to use (2 or 3).")
    .set_default(2)
    .add_service("rgw"),

    Option("rgw_keystone_accepted_roles", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Only users with one of these roles will be served when doing Keystone authentication.")
    .set_default("Member, admin")
    .add_service("rgw"),

    Option("rgw_keystone_accepted_admin_roles", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("List of roles allowing user to gain admin privileges (Keystone).")
    .add_service("rgw"),

    Option("rgw_keystone_accepted_reader_roles", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("List of roles that can only be used for reads (Keystone).")
    .add_service("rgw"),

    Option("rgw_keystone_token_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Keystone token cache size")
    .set_long_description("Max number of Keystone tokens that will be cached. Token that is not cached requires RGW to access the Keystone server when authenticating.")
    .set_default(10000)
    .add_service("rgw"),

    Option("rgw_keystone_verify_ssl", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Should RGW verify the Keystone server SSL certificate.")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_keystone_implicit_tenants", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("RGW Keystone implicit tenants creation")
    .set_long_description("Implicitly create new users in their own tenant with the same name when authenticating via Keystone.  Can be limited to s3 or swift only.")
    .set_default("false")
    .set_enum_allowed({"false", "true", "swift", "s3", "both", "0", "1", "none"})
    .add_service("rgw"),

    Option("rgw_cross_domain_policy", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("RGW handle cross domain policy")
    .set_long_description("Returned cross domain policy when accessing the crossdomain.xml resource (Swift compatiility).")
    .set_default("<allow-access-from domain=\"*\" secure=\"false\" />")
    .add_service("rgw"),

    Option("rgw_healthcheck_disabling_path", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("Swift health check api can be disabled if a file can be accessed in this path.")
    .add_service("rgw"),

    Option("rgw_s3_auth_use_rados", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Should S3 authentication use credentials stored in RADOS backend.")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_s3_auth_use_keystone", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Should S3 authentication use Keystone.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_s3_auth_order", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Authentication strategy order to use for s3 authentication")
    .set_long_description("Order of authentication strategies to try for s3 authentication, the allowed options are a comma separated list of engines external, local. The default order is to try all the externally configured engines before attempting local rados based authentication")
    .set_default("sts, external, local")
    .add_service("rgw"),

    Option("rgw_barbican_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("URL to barbican server.")
    .add_service("rgw"),

    Option("rgw_ldap_uri", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Space-separated list of LDAP servers in URI format.")
    .set_default("ldaps://<ldap.your.domain>")
    .add_service("rgw"),

    Option("rgw_ldap_binddn", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("LDAP entry RGW will bind with (user match).")
    .set_default("uid=admin,cn=users,dc=example,dc=com")
    .add_service("rgw"),

    Option("rgw_ldap_searchdn", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("LDAP search base (basedn).")
    .set_default("cn=users,cn=accounts,dc=example,dc=com")
    .add_service("rgw"),

    Option("rgw_ldap_dnattr", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("LDAP attribute containing RGW user names (to form binddns).")
    .set_default("uid")
    .add_service("rgw"),

    Option("rgw_ldap_secret", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Path to file containing credentials for rgw_ldap_binddn.")
    .set_default("/etc/openldap/secret")
    .add_service("rgw"),

    Option("rgw_s3_auth_use_ldap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Should S3 authentication use LDAP.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_ldap_searchfilter", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("LDAP search filter.")
    .add_service("rgw"),

    Option("rgw_opa_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("URL to OPA server.")
    .add_service("rgw"),

    Option("rgw_opa_token", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("The Bearer token OPA uses to authenticate client requests.")
    .add_service("rgw"),

    Option("rgw_opa_verify_ssl", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Should RGW verify the OPA server SSL certificate.")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_use_opa_authz", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Should OPA be used to authorize client requests.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_admin_entry", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Path prefix to be used for accessing RGW RESTful admin API.")
    .set_default("admin")
    .add_service("rgw"),

    Option("rgw_enforce_swift_acls", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("RGW enforce swift acls")
    .set_long_description("Should RGW enforce special Swift-only ACLs. Swift has a special ACL that gives permission to access all objects in a container.")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_swift_token_expiration", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Expiration time (in seconds) for token generated through RGW Swift auth.")
    .set_default(1_day)
    .add_service("rgw"),

    Option("rgw_print_continue", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("RGW support of 100-continue")
    .set_long_description("Should RGW explicitly send 100 (continue) responses. This is mainly relevant when using FastCGI, as some FastCGI modules do not fully support this feature.")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_print_prohibited_content_length", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("RGW RFC-7230 compatibility")
    .set_long_description("Specifies whether RGW violates RFC 7230 and sends Content-Length with 204 or 304 statuses.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_remote_addr_param", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("HTTP header that holds the remote address in incoming requests.")
    .set_long_description("RGW will use this header to extract requests origin. When RGW runs behind a reverse proxy, the remote address header will point at the proxy's address and not at the originator's address. Therefore it is sometimes possible to have the proxy add the originator's address in a separate HTTP header, which will allow RGW to log it correctly.")
    .set_default("REMOTE_ADDR")
    .add_service("rgw")
    .add_see_also({"rgw_enable_ops_log"}),

    Option("rgw_op_thread_timeout", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("Timeout for async rados coroutine operations.")
    .set_default(10_min)
    .add_service("rgw"),

    Option("rgw_op_thread_suicide_timeout", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("rgw"),

    Option("rgw_thread_pool_size", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_description("RGW requests handling thread pool size.")
    .set_long_description("This parameter determines the number of concurrent requests RGW can process when using either the civetweb, or the fastcgi frontends. The higher this number is, RGW will be able to deal with more concurrent requests at the cost of more resource utilization.")
    .set_default(512)
    .add_service("rgw"),

    Option("rgw_num_control_oids", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Number of control objects used for cross-RGW communication.")
    .set_long_description("RGW uses certain control objects to send messages between different RGW processes running on the same zone. These messages include metadata cache invalidation info that is being sent when metadata is modified (such as user or bucket information). A higher number of control objects allows better concurrency of these messages, at the cost of more resource utilization.")
    .set_default(8)
    .add_service("rgw"),

    Option("rgw_verify_ssl", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Should RGW verify SSL when connecing to a remote HTTP server")
    .set_long_description("RGW can send requests to other RGW servers (e.g., in multi-site sync work). This configurable selects whether RGW should verify the certificate for the remote peer and host.")
    .set_default(true)
    .add_service("rgw")
    .add_see_also({"rgw_keystone_verify_ssl"}),

    Option("rgw_nfs_lru_lanes", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .add_service("rgw"),

    Option("rgw_nfs_lru_lane_hiwat", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(911)
    .add_service("rgw"),

    Option("rgw_nfs_fhcache_partitions", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(3)
    .add_service("rgw"),

    Option("rgw_nfs_fhcache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2017)
    .add_service("rgw"),

    Option("rgw_nfs_namespace_expire_secs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5_min)
    .set_min(1)
    .add_service("rgw"),

    Option("rgw_nfs_max_gc", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5_min)
    .set_min(1)
    .add_service("rgw"),

    Option("rgw_nfs_write_completion_interval_s", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .add_service("rgw"),

    Option("rgw_nfs_s3_fast_attrs", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("use fast S3 attrs from bucket index (immutable only)")
    .set_long_description("use fast S3 attrs from bucket index (assumes NFS mounts are immutable)")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_nfs_run_gc_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("run GC threads in librgw (default off)")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_nfs_run_lc_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("run lifecycle threads in librgw (default off)")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_nfs_run_quota_threads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("run quota threads in librgw (default off)")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_nfs_run_sync_thread", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("run sync thread in librgw (default off)")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_nfs_frontends", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("RGW frontends configuration when running as librgw/nfs")
    .set_long_description("A comma-delimited list of frontends configuration. Each configuration contains the type of the frontend followed by an optional space delimited set of key=value config parameters.")
    .set_default("rgw-nfs")
    .add_service("rgw")
    .add_see_also({"rgw_frontends"}),

    Option("rgw_rados_pool_autoscale_bias", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("pg_autoscale_bias value for RGW metadata (omap-heavy) pools")
    .set_default(4.0)
    .set_min_max(0.01, 100000.0)
    .add_service("rgw"),

    Option("rgw_rados_pool_recovery_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("recovery_priority value for RGW metadata (omap-heavy) pools")
    .set_default(5)
    .set_min_max(-10, 10)
    .add_service("rgw"),

    Option("rgw_zone", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Zone name")
    .add_service("rgw")
    .add_see_also({"rgw_zonegroup", "rgw_realm"}),

    Option("rgw_zone_id", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Zone ID")
    .add_service("rgw")
    .add_see_also({"rgw_zone", "rgw_zonegroup", "rgw_realm"}),

    Option("rgw_zone_root_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Zone root pool name")
    .set_long_description("The zone root pool, is the pool where the RGW zone configuration located.")
    .set_default(".rgw.root")
    .add_service("rgw")
    .add_see_also({"rgw_zonegroup_root_pool", "rgw_realm_root_pool", "rgw_period_root_pool"}),

    Option("rgw_default_zone_info_oid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Default zone info object id")
    .set_long_description("Name of the RADOS object that holds the default zone information.")
    .set_default("default.zone")
    .add_service("rgw"),

    Option("rgw_region", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Region name")
    .set_long_description("Obsolete config option. The rgw_zonegroup option should be used instead.")
    .add_service("rgw")
    .add_see_also({"rgw_zonegroup"}),

    Option("rgw_region_root_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Region root pool")
    .set_long_description("Obsolete config option. The rgw_zonegroup_root_pool should be used instead.")
    .set_default(".rgw.root")
    .add_service("rgw")
    .add_see_also({"rgw_zonegroup_root_pool"}),

    Option("rgw_default_region_info_oid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Default region info object id")
    .set_long_description("Obsolete config option. The rgw_default_zonegroup_info_oid should be used instead.")
    .set_default("default.region")
    .add_service("rgw")
    .add_see_also({"rgw_default_zonegroup_info_oid"}),

    Option("rgw_zonegroup", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Zonegroup name")
    .add_service("rgw")
    .add_see_also({"rgw_zone", "rgw_realm"}),

    Option("rgw_zonegroup_id", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Zonegroup ID")
    .add_service("rgw")
    .add_see_also({"rgw_zone", "rgw_zonegroup", "rgw_realm"}),

    Option("rgw_zonegroup_root_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Zonegroup root pool")
    .set_long_description("The zonegroup root pool, is the pool where the RGW zonegroup configuration located.")
    .set_default(".rgw.root")
    .add_service("rgw")
    .add_see_also({"rgw_zone_root_pool", "rgw_realm_root_pool", "rgw_period_root_pool"}),

    Option("rgw_default_zonegroup_info_oid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default.zonegroup")
    .add_service("rgw"),

    Option("rgw_realm", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .add_service("rgw"),

    Option("rgw_realm_id", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .add_service("rgw"),

    Option("rgw_realm_root_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Realm root pool")
    .set_long_description("The realm root pool, is the pool where the RGW realm configuration located.")
    .set_default(".rgw.root")
    .add_service("rgw")
    .add_see_also({"rgw_zonegroup_root_pool", "rgw_zone_root_pool", "rgw_period_root_pool"}),

    Option("rgw_default_realm_info_oid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("default.realm")
    .add_service("rgw"),

    Option("rgw_period_root_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Period root pool")
    .set_long_description("The period root pool, is the pool where the RGW period configuration located.")
    .set_default(".rgw.root")
    .add_service("rgw")
    .add_see_also({"rgw_zonegroup_root_pool", "rgw_zone_root_pool", "rgw_realm_root_pool"}),

    Option("rgw_period_latest_epoch_info_oid", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default(".latest_epoch")
    .add_service("rgw"),

    Option("rgw_log_nonexistent_bucket", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Should RGW log operations on bucket that does not exist")
    .set_long_description("This config option applies to the ops log. When this option is set, the ops log will log operations that are sent to non existing buckets. These operations inherently fail, and do not correspond to a specific user.")
    .set_default(false)
    .add_service("rgw")
    .add_see_also({"rgw_enable_ops_log"}),

    Option("rgw_log_object_name", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Ops log object name format")
    .set_long_description("Defines the format of the RADOS objects names that ops log uses to store ops log data")
    .set_default("%Y-%m-%d-%H-%i-%n")
    .add_service("rgw")
    .add_see_also({"rgw_enable_ops_log"}),

    Option("rgw_log_object_name_utc", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Should ops log object name based on UTC")
    .set_long_description("If set, the names of the RADOS objects that hold the ops log data will be based on UTC time zone. If not set, it will use the local time zone.")
    .set_default(false)
    .add_service("rgw")
    .add_see_also({"rgw_enable_ops_log", "rgw_log_object_name"}),

    Option("rgw_usage_max_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Number of shards for usage log.")
    .set_long_description("The number of RADOS objects that RGW will use in order to store the usage log data.")
    .set_default(32)
    .add_service("rgw")
    .add_see_also({"rgw_enable_usage_log"}),

    Option("rgw_usage_max_user_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Number of shards for single user in usage log")
    .set_long_description("The number of shards that a single user will span over in the usage log.")
    .set_default(1)
    .set_min(1)
    .add_service("rgw")
    .add_see_also({"rgw_enable_usage_log"}),

    Option("rgw_enable_ops_log", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable ops log")
    .set_default(false)
    .add_service("rgw")
    .add_see_also({"rgw_log_nonexistent_bucket", "rgw_log_object_name", "rgw_ops_log_rados", "rgw_ops_log_socket_path", "rgw_ops_log_file_path"}),

    Option("rgw_enable_usage_log", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable the usage log")
    .set_default(false)
    .add_service("rgw")
    .add_see_also({"rgw_usage_max_shards"}),

    Option("rgw_ops_log_rados", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Use RADOS for ops log")
    .set_long_description("If set, RGW will store ops log information in RADOS. WARNING, there is no automation to clean up these log entries, so by default they will pile up without bound. This MUST NOT be enabled unless the admin has a strategy to manage and trim these log entries with `radosgw-admin log rm`.")
    .set_default(false)
    .add_service("rgw")
    .add_see_also({"rgw_enable_ops_log", "rgw_log_object_name_utc", "rgw_log_object_name"}),

    Option("rgw_ops_log_socket_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Unix domain socket path for ops log.")
    .set_long_description("Path to unix domain socket that RGW will listen for connection on. When connected, RGW will send ops log data through it.")
    .add_service("rgw")
    .add_see_also({"rgw_enable_ops_log", "rgw_ops_log_data_backlog"}),

    Option("rgw_ops_log_file_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("File-system path for ops log.")
    .set_long_description("Path to file that RGW will log ops logs to. A cephadm deployment will automatically rotate these logs under /var/log/ceph/. Other deployments should arrange for similar log rotation.")
    .set_daemon_default("/var/log/ceph/ops-log-$cluster-$name.log")
    .add_service("rgw")
    .add_see_also({"rgw_enable_ops_log"}),

    Option("rgw_ops_log_data_backlog", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Ops log socket backlog")
    .set_long_description("Maximum amount of data backlog that RGW can keep when ops log is configured to send info through unix domain socket. When data backlog is higher than this, ops log entries will be lost. In order to avoid ops log information loss, the listener needs to clear data (by reading it) quickly enough.")
    .set_default(5_M)
    .add_service("rgw")
    .add_see_also({"rgw_enable_ops_log", "rgw_ops_log_socket_path"}),

    Option("rgw_usage_log_flush_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Number of entries in usage log before flushing")
    .set_long_description("This is the max number of entries that will be held in the usage log, before it will be flushed to the backend. Note that the usage log is periodically flushed, even if number of entries does not reach this threshold. A usage log entry corresponds to one or more operations on a single bucket.i")
    .set_default(1024)
    .add_service("rgw")
    .add_see_also({"rgw_enable_usage_log", "rgw_usage_log_tick_interval"}),

    Option("rgw_usage_log_tick_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Number of seconds between usage log flush cycles")
    .set_long_description("The number of seconds between consecutive usage log flushes. The usage log will also flush itself to the backend if the number of pending entries reaches a certain threshold.")
    .set_default(30)
    .add_service("rgw")
    .add_see_also({"rgw_enable_usage_log", "rgw_usage_log_flush_threshold"}),

    Option("rgw_init_timeout", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_description("Initialization timeout")
    .set_long_description("The time length (in seconds) that RGW will allow for its initialization. RGW process will give up and quit if initialization is not complete after this amount of time.")
    .set_default(5_min)
    .add_service("rgw"),

    Option("rgw_mime_types_file", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("Path to local mime types file")
    .set_long_description("The mime types file is needed in Swift when uploading an object. If object's content type is not specified, RGW will use data from this file to assign a content type to the object.")
    .set_default("/etc/mime.types")
    .add_service("rgw"),

    Option("rgw_gc_max_objs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Number of shards for garbage collector data")
    .set_long_description("The number of garbage collector data shards, is the number of RADOS objects that RGW will use to store the garbage collection information on.")
    .set_default(32)
    .add_service("rgw")
    .add_see_also({"rgw_gc_obj_min_wait", "rgw_gc_processor_max_time", "rgw_gc_processor_period", "rgw_gc_max_concurrent_io"}),

    Option("rgw_gc_obj_min_wait", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Garbage collection object expiration time")
    .set_long_description("The length of time (in seconds) that the RGW collector will wait before purging a deleted object's data. RGW will not remove object immediately, as object could still have readers. A mechanism exists to increase the object's expiration time when it's being read. The recommended value of its lower limit is 30 minutes")
    .set_default(2_hr)
    .add_service("rgw")
    .add_see_also({"rgw_gc_max_objs", "rgw_gc_processor_max_time", "rgw_gc_processor_period", "rgw_gc_max_concurrent_io"}),

    Option("rgw_gc_processor_max_time", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Length of time GC processor can lease shard")
    .set_long_description("Garbage collection thread in RGW process holds a lease on its data shards. These objects contain the information about the objects that need to be removed. RGW takes a lease in order to prevent multiple RGW processes from handling the same objects concurrently. This time signifies that maximum amount of time (in seconds) that RGW is allowed to hold that lease. In the case where RGW goes down uncleanly, this is the amount of time where processing of that data shard will be blocked.")
    .set_default(1_hr)
    .add_service("rgw")
    .add_see_also({"rgw_gc_max_objs", "rgw_gc_obj_min_wait", "rgw_gc_processor_period", "rgw_gc_max_concurrent_io"}),

    Option("rgw_gc_processor_period", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Garbage collector cycle run time")
    .set_long_description("The amount of time between the start of consecutive runs of the garbage collector threads. If garbage collector runs takes more than this period, it will not wait before running again.")
    .set_default(1_hr)
    .add_service("rgw")
    .add_see_also({"rgw_gc_max_objs", "rgw_gc_obj_min_wait", "rgw_gc_processor_max_time", "rgw_gc_max_concurrent_io", "rgw_gc_max_trim_chunk"}),

    Option("rgw_gc_max_concurrent_io", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Max concurrent RADOS IO operations for garbage collection")
    .set_long_description("The maximum number of concurrent IO operations that the RGW garbage collection thread will use when purging old data.")
    .set_default(10)
    .add_service("rgw")
    .add_see_also({"rgw_gc_max_objs", "rgw_gc_obj_min_wait", "rgw_gc_processor_max_time", "rgw_gc_max_trim_chunk"}),

    Option("rgw_gc_max_trim_chunk", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Max number of keys to remove from garbage collector log in a single operation")
    .set_default(16)
    .add_service("rgw")
    .add_see_also({"rgw_gc_max_objs", "rgw_gc_obj_min_wait", "rgw_gc_processor_max_time", "rgw_gc_max_concurrent_io"}),

    Option("rgw_gc_max_deferred_entries_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum allowed size of deferred entries in queue head for gc")
    .set_default(3_K)
    .add_service("rgw"),

    Option("rgw_gc_max_queue_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Maximum allowed queue size for gc")
    .set_long_description("The maximum allowed size of each gc queue, and its value should not be greater than (osd_max_object_size - rgw_gc_max_deferred_entries_size - 1K).")
    .set_default(131068_K)
    .add_service("rgw")
    .add_see_also({"osd_max_object_size", "rgw_gc_max_deferred_entries_size"}),

    Option("rgw_gc_max_deferred", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of maximum deferred data entries to be stored in queue for gc")
    .set_default(50)
    .add_service("rgw"),

    Option("rgw_s3_success_create_obj_status", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("HTTP return code override for object creation")
    .set_long_description("If not zero, this is the HTTP return code that will be returned on a successful S3 object creation.")
    .set_default(0)
    .add_service("rgw"),

    Option("rgw_s3_client_max_sig_ver", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Max S3 authentication signature version")
    .set_long_description("If greater than zero, would force max signature version to use")
    .set_default(-1)
    .add_service("rgw"),

    Option("rgw_resolve_cname", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Support vanity domain names via CNAME")
    .set_long_description("If true, RGW will query DNS when detecting that it's serving a request that was sent to a host in another domain. If a CNAME record is configured for that domain it will use it instead. This gives user to have the ability of creating a unique domain of their own to point at data in their bucket.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_obj_stripe_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("RGW object stripe size")
    .set_long_description("The size of an object stripe for RGW objects. This is the maximum size a backing RADOS object will have. RGW objects that are larger than this will span over multiple objects.")
    .set_default(4_M)
    .add_service("rgw"),

    Option("rgw_extended_http_attrs", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("RGW support extended HTTP attrs")
    .set_long_description("Add new set of attributes that could be set on an object. These extra attributes can be set through HTTP header fields when putting the objects. If set, these attributes will return as HTTP fields when doing GET/HEAD on the object.")
    .add_service("rgw"),

    Option("rgw_exit_timeout_secs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("RGW shutdown timeout")
    .set_long_description("Number of seconds to wait for a process before exiting unconditionally.")
    .set_default(2_min)
    .add_service("rgw"),

    Option("rgw_get_obj_window_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("RGW object read window size")
    .set_long_description("The window size in bytes for a single object read request")
    .set_default(16_M)
    .add_service("rgw"),

    Option("rgw_get_obj_max_req_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("RGW object read chunk size")
    .set_long_description("The maximum request size of a single object read operation sent to RADOS")
    .set_default(4_M)
    .add_service("rgw"),

    Option("rgw_relaxed_s3_bucket_names", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("RGW enable relaxed S3 bucket names")
    .set_long_description("RGW enable relaxed S3 bucket name rules for US region buckets.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_defer_to_bucket_acls", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Bucket ACLs override object ACLs")
    .set_long_description("If not empty, a string that selects that mode of operation. 'recurse' will use bucket's ACL for the authorization. 'full-control' will allow users that users that have full control permission on the bucket have access to the object.")
    .add_service("rgw"),

    Option("rgw_list_buckets_max_chunk", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Max number of buckets to retrieve in a single listing operation")
    .set_long_description("When RGW fetches lists of user's buckets from the backend, this is the max number of entries it will try to retrieve in a single operation. Note that the backend may choose to return a smaller number of entries.")
    .set_default(1000)
    .add_service("rgw"),

    Option("rgw_md_log_max_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("RGW number of metadata log shards")
    .set_long_description("The number of shards the RGW metadata log entries will reside in. This affects the metadata sync parallelism as a shard can only be processed by a single RGW at a time")
    .set_default(64)
    .add_service("rgw"),

    Option("rgw_curl_buffersize", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_long_description("Pass a long specifying your preferred size (in bytes) for the receivebuffer in libcurl. See: https://curl.se/libcurl/c/CURLOPT_BUFFERSIZE.html")
    .set_default(524288)
    .set_min_max(1024, 524288)
    .add_service("rgw"),

    Option("rgw_curl_wait_timeout_ms", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(1000)
    .add_service("rgw"),

    Option("rgw_curl_low_speed_limit", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_long_description("It contains the average transfer speed in bytes per second that the transfer should be below during rgw_curl_low_speed_time seconds for libcurl to consider it to be too slow and abort. Set it zero to disable this.")
    .set_default(1024)
    .add_service("rgw"),

    Option("rgw_curl_low_speed_time", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_long_description("It contains the time in number seconds that the transfer speed should be below the rgw_curl_low_speed_limit for the library to consider it too slow and abort. Set it zero to disable this.")
    .set_default(5_min)
    .add_service("rgw"),

    Option("rgw_curl_tcp_keepalive", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_long_description("Enable TCP keepalive on the HTTP client sockets managed by libcurl. This does not apply to connections received by the HTTP frontend, but only to HTTP requests sent by radosgw. Examples include requests to Keystone for authentication, sync requests from multisite, and requests to key management servers for SSE.")
    .set_default(0)
    .set_enum_allowed({"0", "1"})
    .add_service("rgw"),

    Option("rgw_copy_obj_progress", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Send progress report through copy operation")
    .set_long_description("If true, RGW will send progress information when copy operation is executed.")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_copy_obj_progress_every_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Send copy-object progress info after these many bytes")
    .set_default(1_M)
    .add_service("rgw"),

    Option("rgw_max_copy_obj_concurrent_io", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Number of refcount operations to process concurrently when executing copy_obj")
    .set_default(10)
    .add_service("rgw"),

    Option("rgw_sync_obj_etag_verify", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Verify if the object copied from remote is identical to its source")
    .set_long_description("If true, this option computes the MD5 checksum of the data which is written at the destination and checks if it is identical to the ETAG stored in the source. It ensures integrity of the objects fetched from a remote server over HTTP including multisite sync.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_obj_tombstone_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Max number of entries to keep in tombstone cache")
    .set_long_description("The tombstone cache is used when doing a multi-zone data sync. RGW keeps there information about removed objects which is needed in order to prevent re-syncing of objects that were already removed.")
    .set_default(1000)
    .add_service("rgw"),

    Option("rgw_data_log_window", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Data log time window")
    .set_long_description("The data log keeps information about buckets that have objectst that were modified within a specific timeframe. The sync process then knows which buckets are needed to be scanned for data sync.")
    .set_default(30)
    .add_service("rgw"),

    Option("rgw_data_log_changes_size", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("Max size of pending changes in data log")
    .set_long_description("RGW will trigger update to the data log if the number of pending entries reached this number.")
    .set_default(1000)
    .add_service("rgw"),

    Option("rgw_data_log_num_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Number of data log shards")
    .set_long_description("The number of shards the RGW data log entries will reside in. This affects the data sync parallelism as a shard can only be processed by a single RGW at a time.")
    .set_default(128)
    .add_service("rgw"),

    Option("rgw_data_log_obj_prefix", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_default("data_log")
    .add_service("rgw"),

    Option("rgw_data_sync_poll_interval", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(20)
    .add_service("rgw")
    .add_see_also({"rgw_meta_sync_poll_interval"}),

    Option("rgw_meta_sync_poll_interval", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(20)
    .add_service("rgw")
    .add_see_also({"rgw_data_sync_poll_interval"}),

    Option("rgw_bucket_sync_spawn_window", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(20)
    .add_service("rgw")
    .add_see_also({"rgw_data_sync_spawn_window", "rgw_meta_sync_spawn_window"}),

    Option("rgw_data_sync_spawn_window", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(20)
    .add_service("rgw")
    .add_see_also({"rgw_bucket_sync_spawn_window", "rgw_meta_sync_spawn_window"}),

    Option("rgw_meta_sync_spawn_window", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(20)
    .add_service("rgw")
    .add_see_also({"rgw_bucket_sync_spawn_window", "rgw_data_sync_spawn_window"}),

    Option("rgw_bucket_quota_ttl", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Bucket quota stats cache TTL")
    .set_long_description("Length of time for bucket stats to be cached within RGW instance.")
    .set_default(10_min)
    .add_service("rgw"),

    Option("rgw_bucket_quota_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("RGW quota stats cache size")
    .set_long_description("Maximum number of entries in the quota stats cache.")
    .set_default(10000)
    .add_service("rgw"),

    Option("rgw_bucket_default_quota_max_objects", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_description("Default quota for max objects in a bucket")
    .set_long_description("The default quota configuration for max number of objects in a bucket. A negative number means 'unlimited'.")
    .set_default(-1)
    .add_service("rgw"),

    Option("rgw_bucket_default_quota_max_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Default quota for total size in a bucket")
    .set_long_description("The default quota configuration for total size of objects in a bucket. A negative number means 'unlimited'.")
    .set_default(-1)
    .add_service("rgw"),

    Option("rgw_expose_bucket", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Send Bucket HTTP header with the response")
    .set_long_description("If true, RGW will send a Bucket HTTP header with the responses. The header will contain the name of the bucket the operation happened on.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_frontends", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("RGW frontends configuration")
    .set_long_description("A comma delimited list of frontends configuration. Each configuration contains the type of the frontend followed by an optional space delimited set of key=value config parameters.")
    .set_default("beast port=7480")
    .add_service("rgw"),

    Option("rgw_frontend_defaults", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("RGW frontends default configuration")
    .set_long_description("A comma delimited list of default frontends configuration.")
    .set_default("beast ssl_certificate=config://rgw/cert/$realm/$zone.crt ssl_private_key=config://rgw/cert/$realm/$zone.key")
    .add_service("rgw"),

    Option("rgw_beast_enable_async", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Enable async request processing under beast using coroutines")
    .set_long_description("When enabled, the beast frontend will process requests using coroutines, allowing the concurrent processing of several requests on the same thread. When disabled, the number of concurrent requests will be limited by the thread count, but debugging and tracing the synchronous calls can be easier.")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_user_quota_bucket_sync_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("User quota bucket sync interval")
    .set_long_description("Time period for accumulating modified buckets before syncing these stats.")
    .set_default(3_min)
    .add_service("rgw"),

    Option("rgw_user_quota_sync_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("User quota sync interval")
    .set_long_description("Time period for accumulating modified buckets before syncing entire user stats.")
    .set_default(1_day)
    .add_service("rgw"),

    Option("rgw_user_quota_sync_idle_users", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Should sync idle users quota")
    .set_long_description("Whether stats for idle users be fully synced.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_user_quota_sync_wait_time", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("User quota full-sync wait time")
    .set_long_description("Minimum time between two full stats sync for non-idle users.")
    .set_default(1_day)
    .add_service("rgw"),

    Option("rgw_user_default_quota_max_objects", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_description("User quota max objects")
    .set_long_description("The default quota configuration for total number of objects for a single user. A negative number means 'unlimited'.")
    .set_default(-1)
    .add_service("rgw"),

    Option("rgw_user_default_quota_max_size", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_description("User quota max size")
    .set_long_description("The default quota configuration for total size of objects for a single user. A negative number means 'unlimited'.")
    .set_default(-1)
    .add_service("rgw"),

    Option("rgw_multipart_min_part_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Minimum S3 multipart-upload part size")
    .set_long_description("When doing a multipart upload, each part (other than the last part) must be at least this size.")
    .set_default(5_M)
    .add_service("rgw"),

    Option("rgw_multipart_part_upload_limit", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Max number of parts in multipart upload")
    .set_default(10000)
    .add_service("rgw"),

    Option("rgw_max_slo_entries", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Max number of entries in Swift Static Large Object manifest")
    .set_default(1000)
    .add_service("rgw"),

    Option("rgw_olh_pending_timeout_sec", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("Max time for pending OLH change to complete")
    .set_long_description("OLH is a versioned object's logical head. Operations on it are journaled and as pending before completion. If an operation doesn't complete with this amount of seconds, we remove the operation from the journal.")
    .set_default(1_hr)
    .add_service("rgw"),

    Option("rgw_user_max_buckets", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_description("Max number of buckets per user")
    .set_long_description("A user can create at most this number of buckets. Zero means no limit; a negative value means users cannot create any new buckets, although users will retain buckets already created.")
    .set_default(1000)
    .add_service("rgw"),

    Option("rgw_objexp_gc_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Swift objects expirer garbage collector interval")
    .set_default(600)
    .add_service("rgw"),

    Option("rgw_objexp_hints_num_shards", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of object expirer data shards")
    .set_long_description("The number of shards the (Swift) object expirer will store its data on.")
    .set_default(127)
    .add_service("rgw"),

    Option("rgw_objexp_chunk_size", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(100)
    .add_service("rgw"),

    Option("rgw_enable_static_website", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("Enable static website APIs")
    .set_long_description("This configurable controls whether RGW handles the website control APIs. RGW can server static websites if s3website hostnames are configured, and unrelated to this configurable.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_user_unique_email", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("Require local RGW users to have unique email addresses")
    .set_long_description("Enforce builtin user accounts to have unique email addresses.  This setting is historical.  In future, non-enforcement of email address uniqueness is likely to become the default.")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_log_http_headers", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("List of HTTP headers to log")
    .set_long_description("A comma delimited list of HTTP headers to log when seen, ignores case (e.g., http_x_forwarded_for).")
    .add_service("rgw"),

    Option("rgw_num_async_rados_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Number of concurrent RADOS operations in multisite sync")
    .set_long_description("The number of concurrent RADOS IO operations that will be triggered for handling multisite sync operations. This includes control related work, and not the actual sync operations.")
    .set_default(32)
    .add_service("rgw"),

    Option("rgw_md_notify_interval_msec", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Length of time to aggregate metadata changes")
    .set_long_description("Length of time (in milliseconds) in which the master zone aggregates all the metadata changes that occurred, before sending notifications to all the other zones.")
    .set_default(200)
    .add_service("rgw"),

    Option("rgw_run_sync_thread", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Should run sync thread")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_sync_lease_period", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(2_min)
    .add_service("rgw"),

    Option("rgw_sync_log_trim_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Sync log trim interval")
    .set_long_description("Time in seconds between attempts to trim sync logs.")
    .set_default(20_min)
    .add_service("rgw"),

    Option("rgw_sync_log_trim_max_buckets", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Maximum number of buckets to trim per interval")
    .set_long_description("The maximum number of buckets to consider for bucket index log trimming each trim interval, regardless of the number of bucket index shards. Priority is given to buckets with the most sync activity over the last trim interval.")
    .set_default(16)
    .add_service("rgw")
    .add_see_also({"rgw_sync_log_trim_interval", "rgw_sync_log_trim_min_cold_buckets", "rgw_sync_log_trim_concurrent_buckets"}),

    Option("rgw_sync_log_trim_min_cold_buckets", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Minimum number of cold buckets to trim per interval")
    .set_long_description("Of the `rgw_sync_log_trim_max_buckets` selected for bucket index log trimming each trim interval, at least this many of them must be 'cold' buckets. These buckets are selected in order from the list of all bucket instances, to guarantee that all buckets will be visited eventually.")
    .set_default(4)
    .add_service("rgw")
    .add_see_also({"rgw_sync_log_trim_interval", "rgw_sync_log_trim_max_buckets", "rgw_sync_log_trim_concurrent_buckets"}),

    Option("rgw_sync_log_trim_concurrent_buckets", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Maximum number of buckets to trim in parallel")
    .set_default(4)
    .add_service("rgw")
    .add_see_also({"rgw_sync_log_trim_interval", "rgw_sync_log_trim_max_buckets", "rgw_sync_log_trim_min_cold_buckets"}),

    Option("rgw_sync_data_inject_err_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0)
    .add_service("rgw"),

    Option("rgw_sync_meta_inject_err_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0)
    .add_service("rgw"),

    Option("rgw_sync_data_full_inject_err_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0)
    .add_service("rgw"),

    Option("rgw_sync_trace_history_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Sync trace history size")
    .set_long_description("Maximum number of complete sync trace entries to keep.")
    .set_default(4_K)
    .add_service("rgw"),

    Option("rgw_sync_trace_per_node_log_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Sync trace per-node log size")
    .set_long_description("The number of log entries to keep per sync-trace node.")
    .set_default(32)
    .add_service("rgw"),

    Option("rgw_sync_trace_servicemap_update_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Sync-trace service-map update interval")
    .set_long_description("Number of seconds between service-map updates of sync-trace events.")
    .set_default(10)
    .add_service("rgw"),

    Option("rgw_period_push_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Period push interval")
    .set_long_description("Number of seconds to wait before retrying 'period push' operation.")
    .set_default(2.0)
    .add_service("rgw"),

    Option("rgw_period_push_interval_max", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Period push maximum interval")
    .set_long_description("The max number of seconds to wait before retrying 'period push' after exponential backoff.")
    .set_default(30.0)
    .add_service("rgw"),

    Option("rgw_safe_max_objects_per_shard", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Safe number of objects per shard")
    .set_long_description("This is the max number of objects per bucket index shard that RGW considers safe. RGW will warn if it identifies a bucket where its per-shard count is higher than a percentage of this number.")
    .set_default(102400)
    .add_service("rgw")
    .add_see_also({"rgw_shard_warning_threshold"}),

    Option("rgw_shard_warning_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Warn about max objects per shard")
    .set_long_description("Warn if number of objects per shard in a specific bucket passed this percentage of the safe number.")
    .set_default(90.0)
    .add_service("rgw")
    .add_see_also({"rgw_safe_max_objects_per_shard"}),

    Option("rgw_swift_versioning_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable Swift versioning")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_swift_custom_header", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Enable swift custom header")
    .set_long_description("If not empty, specifies a name of HTTP header that can include custom data. When uploading an object, if this header is passed RGW will store this header info and it will be available when listing the bucket.")
    .add_service("rgw"),

    Option("rgw_swift_need_stats", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable stats on bucket listing in Swift")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_reshard_num_logs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(16)
    .set_min(1)
    .add_service({"rgw", "rgw"}),

    Option("rgw_reshard_bucket_lock_duration", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of seconds the timeout on the reshard locks (bucket reshard lock and reshard log lock) are set to. As a reshard proceeds these locks can be renewed/extended. If too short, reshards cannot complete and will fail, causing a future reshard attempt. If too long a hung or crashed reshard attempt will keep the bucket locked for an extended period, not allowing RGW to detect the failed reshard attempt and recover.")
    .set_default(360)
    .set_min(30)
    .add_service({"rgw", "rgw"})
    .add_tag("performance"),

    Option("rgw_debug_inject_set_olh_err", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("Whether to inject errors between rados olh modification initialization and bucket index instance linking. The value determines the error code. This exists for development and testing purposes to help simulate cases where bucket index entries aren't cleaned up by the request thread after an error scenario.")
    .set_default(0)
    .add_service("rgw"),

    Option("rgw_debug_inject_olh_cancel_modification_err", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Whether to inject an error to simulate a failure to cancel olh modification. This exists for development and testing purposes.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_reshard_batch_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of reshard entries to batch together before sending the operations to the CLS back-end")
    .set_default(64)
    .set_min(8)
    .add_service({"rgw", "rgw"})
    .add_tag("performance"),

    Option("rgw_reshard_max_aio", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Maximum number of outstanding asynchronous I/O operations to allow at a time during resharding")
    .set_default(128)
    .set_min(16)
    .add_service({"rgw", "rgw"})
    .add_tag("performance"),

    Option("rgw_trust_forwarded_https", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Trust Forwarded and X-Forwarded-Proto headers")
    .set_long_description("When a proxy in front of radosgw is used for ssl termination, radosgw does not know whether incoming http connections are secure. Enable this option to trust the Forwarded and X-Forwarded-Proto headers sent by the proxy when determining whether the connection is secure. This is required for some features, such as server side encryption. (Never enable this setting if you do not have a trusted proxy in front of radosgw, or else malicious users will be able to set these headers in any request.)")
    .set_default(false)
    .add_service("rgw")
    .add_see_also({"rgw_crypt_require_ssl"}),

    Option("rgw_crypt_require_ssl", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Requests including encryption key headers must be sent over ssl")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_crypt_default_encryption_key", Option::TYPE_STR, Option::LEVEL_DEV)
    .add_service("rgw"),

    Option("rgw_crypt_s3_kms_backend", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Where the SSE-KMS encryption keys are stored. Supported KMS systems are OpenStack Barbican ('barbican', the default) and HashiCorp Vault ('vault').")
    .set_default("barbican")
    .set_enum_allowed({"barbican", "vault", "testing", "kmip"})
    .add_service("rgw"),

    Option("rgw_crypt_s3_kms_encryption_keys", Option::TYPE_STR, Option::LEVEL_DEV)
    .add_service("rgw"),

    Option("rgw_crypt_vault_auth", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Type of authentication method to be used with Vault.")
    .set_default("token")
    .set_enum_allowed({"token", "agent"})
    .add_service("rgw")
    .add_see_also({"rgw_crypt_s3_kms_backend", "rgw_crypt_vault_addr", "rgw_crypt_vault_token_file"}),

    Option("rgw_crypt_vault_token_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("If authentication method is 'token', provide a path to the token file, which for security reasons should readable only by Rados Gateway.")
    .add_service("rgw")
    .add_see_also({"rgw_crypt_s3_kms_backend", "rgw_crypt_vault_auth", "rgw_crypt_vault_addr"}),

    Option("rgw_crypt_vault_addr", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Vault server base address.")
    .add_service("rgw")
    .add_see_also({"rgw_crypt_s3_kms_backend", "rgw_crypt_vault_auth", "rgw_crypt_vault_prefix"}),

    Option("rgw_crypt_vault_prefix", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Vault secret URL prefix, which can be used to restrict access to a particular subset of the Vault secret space.")
    .add_service("rgw")
    .add_see_also({"rgw_crypt_s3_kms_backend", "rgw_crypt_vault_addr", "rgw_crypt_vault_auth"}),

    Option("rgw_crypt_vault_secret_engine", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Vault Secret Engine to be used to retrieve encryption keys.")
    .set_default("transit")
    .add_service("rgw")
    .add_see_also({"rgw_crypt_s3_kms_backend", "rgw_crypt_vault_auth", "rgw_crypt_vault_addr"}),

    Option("rgw_crypt_vault_namespace", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Vault Namespace to be used to select your tenant")
    .add_service("rgw")
    .add_see_also({"rgw_crypt_s3_kms_backend", "rgw_crypt_vault_auth", "rgw_crypt_vault_addr"}),

    Option("rgw_crypt_vault_verify_ssl", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Should RGW verify the vault server SSL certificate.")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_crypt_vault_ssl_cacert", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Path for custom ca certificate for accessing vault server")
    .add_service("rgw"),

    Option("rgw_crypt_vault_ssl_clientcert", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Path for custom client certificate for accessing vault server")
    .add_service("rgw"),

    Option("rgw_crypt_vault_ssl_clientkey", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Path for private key required for client cert")
    .add_service("rgw"),

    Option("rgw_crypt_kmip_addr", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("kmip server address")
    .add_service("rgw"),

    Option("rgw_crypt_kmip_ca_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("ca for kmip servers")
    .add_service("rgw"),

    Option("rgw_crypt_kmip_username", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("when authenticating via username")
    .add_service("rgw"),

    Option("rgw_crypt_kmip_password", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("optional w/ username")
    .add_service("rgw"),

    Option("rgw_crypt_kmip_client_cert", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("connect using client certificate")
    .add_service("rgw"),

    Option("rgw_crypt_kmip_client_key", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("connect using client certificate")
    .add_service("rgw"),

    Option("rgw_crypt_kmip_kms_key_template", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("sse-kms; kmip key names")
    .add_service("rgw"),

    Option("rgw_crypt_kmip_s3_key_template", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("sse-s3; kmip key template")
    .set_default("$keyid")
    .add_service("rgw"),

    Option("rgw_crypt_suppress_logs", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Suppress logs that might print client key")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_crypt_sse_s3_backend", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Where the SSE-S3 encryption keys are stored. The only valid choice here is HashiCorp Vault ('vault').")
    .set_default("vault")
    .set_enum_allowed({"vault"})
    .add_service("rgw"),

    Option("rgw_crypt_sse_s3_vault_secret_engine", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Vault Secret Engine to be used to retrieve encryption keys.")
    .set_default("transit")
    .add_service("rgw")
    .add_see_also({"rgw_crypt_sse_s3_backend", "rgw_crypt_sse_s3_vault_auth", "rgw_crypt_sse_s3_vault_addr"}),

    Option("rgw_crypt_sse_s3_key_template", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("template for per-bucket sse-s3 keys in vault.")
    .set_long_description("This is the template for per-bucket sse-s3 keys. This string may include ``%bucket_id`` which will be expanded out to the bucket marker, a unique uuid assigned to that bucket. It could contain ``%owner_id``, which will expand out to the owner's id. Any other use of % is reserved and should not be used. If the template contains ``%bucket_id``, associated bucket keys will be automatically removed when the bucket is removed.")
    .set_default("%bucket_id")
    .add_service("rgw")
    .add_see_also({"rgw_crypt_sse_s3_backend", "rgw_crypt_sse_s3_vault_auth", "rgw_crypt_sse_s3_vault_addr"}),

    Option("rgw_crypt_sse_s3_vault_auth", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Type of authentication method to be used with SSE-S3 and Vault.")
    .set_default("token")
    .set_enum_allowed({"token", "agent"})
    .add_service("rgw")
    .add_see_also({"rgw_crypt_sse_s3_backend", "rgw_crypt_sse_s3_vault_addr", "rgw_crypt_sse_s3_vault_token_file"}),

    Option("rgw_crypt_sse_s3_vault_token_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("If authentication method is 'token', provide a path to the token file, which for security reasons should readable only by Rados Gateway.")
    .add_service("rgw")
    .add_see_also({"rgw_crypt_sse_s3_backend", "rgw_crypt_sse_s3_vault_auth", "rgw_crypt_sse_s3_vault_addr"}),

    Option("rgw_crypt_sse_s3_vault_addr", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("SSE-S3 Vault server base address.")
    .add_service("rgw")
    .add_see_also({"rgw_crypt_sse_s3_backend", "rgw_crypt_sse_s3_vault_auth", "rgw_crypt_sse_s3_vault_prefix"}),

    Option("rgw_crypt_sse_s3_vault_prefix", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("SSE-S3 Vault secret URL prefix, which can be used to restrict access to a particular subset of the Vault secret space.")
    .add_service("rgw")
    .add_see_also({"rgw_crypt_sse_s3_backend", "rgw_crypt_sse_s3_vault_addr", "rgw_crypt_sse_s3_vault_auth"}),

    Option("rgw_crypt_sse_s3_vault_namespace", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Vault Namespace to be used to select your tenant")
    .add_service("rgw")
    .add_see_also({"rgw_crypt_sse_s3_backend", "rgw_crypt_sse_s3_vault_auth", "rgw_crypt_sse_s3_vault_addr"}),

    Option("rgw_crypt_sse_s3_vault_verify_ssl", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Should RGW verify the vault server SSL certificate.")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_crypt_sse_s3_vault_ssl_cacert", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Path for custom ca certificate for accessing vault server")
    .add_service("rgw"),

    Option("rgw_crypt_sse_s3_vault_ssl_clientcert", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Path for custom client certificate for accessing vault server")
    .add_service("rgw"),

    Option("rgw_crypt_sse_s3_vault_ssl_clientkey", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Path for private key required for client cert")
    .add_service("rgw"),

    Option("rgw_list_bucket_min_readahead", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Minimum number of entries to request from rados for bucket listing")
    .set_default(1000)
    .add_service("rgw"),

    Option("rgw_rest_getusage_op_compat", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("REST GetUsage request backward compatibility")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_torrent_flag", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("When true, uploaded objects will calculate and store a SHA256 hash of object data so the object can be retrieved as a torrent file")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_torrent_tracker", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Torrent field announce and announce list")
    .add_service("rgw"),

    Option("rgw_torrent_createby", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("torrent field created by")
    .add_service("rgw"),

    Option("rgw_torrent_comment", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Torrent field comment")
    .add_service("rgw"),

    Option("rgw_torrent_encoding", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("torrent field encoding")
    .add_service("rgw"),

    Option("rgw_data_notify_interval_msec", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("data changes notification interval to followers")
    .set_long_description("In multisite, radosgw will occasionally broadcast new entries in its data changes log to peer zones, so they can prioritize sync of some of the most recent changes. Can be disabled with 0.")
    .set_default(0)
    .add_service("rgw"),

    Option("rgw_torrent_origin", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Torrent origin")
    .add_service("rgw"),

    Option("rgw_torrent_sha_unit", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(512_K)
    .add_service("rgw"),

    Option("rgw_torrent_max_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Objects over this size will not store torrent info.")
    .set_default(5_G)
    .add_service("rgw")
    .add_see_also({"rgw_torrent_flag"}),

    Option("rgw_dynamic_resharding", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("Enable dynamic resharding")
    .set_long_description("If true, RGW will dynamically increase the number of shards in buckets that have a high number of objects per shard.")
    .set_default(true)
    .add_service("rgw")
    .add_see_also({"rgw_max_objs_per_shard", "rgw_max_dynamic_shards"}),

    Option("rgw_max_objs_per_shard", Option::TYPE_UINT, Option::LEVEL_BASIC)
    .set_description("Max objects per shard for dynamic resharding")
    .set_long_description("This is the max number of objects per bucket index shard that RGW will allow with dynamic resharding. RGW will trigger an automatic reshard operation on the bucket if it exceeds this number.")
    .set_default(100000)
    .add_service("rgw")
    .add_see_also({"rgw_dynamic_resharding", "rgw_max_dynamic_shards"}),

    Option("rgw_max_dynamic_shards", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Max shards that dynamic resharding can create")
    .set_long_description("This is the maximum number of bucket index shards that dynamic sharding is able to create on its own. This does not limit user requested resharding. Ideally this value is a prime number.")
    .set_default(1999)
    .set_min(1)
    .add_service("rgw")
    .add_see_also({"rgw_dynamic_resharding", "rgw_max_objs_per_shard"}),

    Option("rgw_reshard_thread_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of seconds between processing of reshard log entries")
    .set_default(600)
    .set_min(10)
    .add_service("rgw"),

    Option("rgw_cache_expiry_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of seconds before entries in the cache are assumed stale and re-fetched. Zero is never.")
    .set_long_description("The Rados Gateway stores metadata and objects in an internal cache. This should be kept consistent by the OSD's relaying notify events between multiple watching RGW processes. In the event that this notification protocol fails, bounding the length of time that any data in the cache will be assumed valid will ensure that any RGW instance that falls out of sync will eventually recover. This seems to be an issue mostly for large numbers of RGW instances under heavy use. If you would like to turn off cache expiry, set this value to zero.")
    .set_default(900)
    .add_service({"rgw", "rgw"})
    .add_tag("performance"),

    Option("rgw_inject_notify_timeout_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Likelihood of ignoring a notify")
    .set_long_description("This is the probability that the RGW cache will ignore a cache notify message. It exists to help with the development and testing of cache consistency and recovery improvements. Please do not set it in a production cluster, as it actively causes failures. Set this to a floating point value between 0 and 1.")
    .set_default(0.0)
    .set_min_max(0.0, 1.0)
    .add_service({"rgw", "rgw"})
    .add_tag("fault injection")
    .add_tag("testing"),

    Option("rgw_max_notify_retries", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of attempts to notify peers before giving up.")
    .set_long_description("The number of times we will attempt to update a peer's cache in the event of error before giving up. This is unlikely to be an issue unless your cluster is very heavily loaded. Beware that increasing this value may cause some operations to take longer in exceptional cases and thus may, rarely, cause clients to time out.")
    .set_default(10)
    .add_service({"rgw", "rgw"})
    .add_tag("error recovery"),

    Option("rgw_sts_entry", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("STS URL prefix")
    .set_long_description("URL path prefix for internal STS requests.")
    .set_default("sts")
    .add_service("rgw"),

    Option("rgw_sts_key", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("STS Key")
    .set_long_description("Key used for encrypting/ decrypting session token.")
    .set_default("sts")
    .add_service("rgw"),

    Option("rgw_s3_auth_use_sts", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Should S3 authentication use STS.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_sts_max_session_duration", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Session token max duration")
    .set_long_description("This option can be used to configure the upper limit of the durationSeconds of temporary credentials returned by 'GetSessionToken'.")
    .set_default(43200)
    .add_service("rgw")
    .add_see_also({"rgw_sts_min_session_duration"}),

    Option("rgw_sts_min_session_duration", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Minimum allowed duration of a session")
    .set_long_description("This option can be used to configure the lower limit of durationSeconds of temporary credentials returned by 'AssumeRole*' calls.")
    .set_default(900)
    .add_service("rgw")
    .add_see_also({"rgw_sts_max_session_duration"}),

    Option("rgw_max_listing_results", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Upper bound on results in listing operations, ListBucket max-keys")
    .set_long_description("This caps the maximum permitted value for listing-like operations in RGW S3. Affects ListBucket(max-keys), ListBucketVersions(max-keys), ListBucketMultipartUploads(max-uploads), ListMultipartUploadParts(max-parts)")
    .set_default(1000)
    .set_min_max(1, 100000)
    .add_service({"rgw", "rgw"}),

    Option("rgw_sts_token_introspection_url", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("STS Web Token introspection URL")
    .set_long_description("URL for introspecting an STS Web Token.")
    .add_service("rgw"),

    Option("rgw_sts_client_id", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Client Id")
    .set_long_description("Client Id needed for introspecting a Web Token.")
    .add_service("rgw"),

    Option("rgw_sts_client_secret", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Client Secret")
    .set_long_description("Client Secret needed for introspecting a Web Token.")
    .add_service("rgw"),

    Option("rgw_max_concurrent_requests", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_description("Maximum number of concurrent HTTP requests.")
    .set_long_description("Maximum number of concurrent HTTP requests that the beast frontend will process. Tuning this can help to limit memory usage under heavy load.")
    .set_default(1024)
    .add_service("rgw")
    .add_tag("performance")
    .add_see_also({"rgw_frontends"}),

    Option("rgw_scheduler_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Set the type of dmclock scheduler, defaults to throttler Other valid values are dmclock which is experimental")
    .set_default("throttler")
    .add_service("rgw"),

    Option("rgw_dmclock_admin_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("mclock reservation for admin requests")
    .set_default(100.0)
    .add_service("rgw")
    .add_see_also({"rgw_dmclock_admin_wgt", "rgw_dmclock_admin_lim"}),

    Option("rgw_dmclock_admin_wgt", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("mclock weight for admin requests")
    .set_default(100.0)
    .add_service("rgw")
    .add_see_also({"rgw_dmclock_admin_res", "rgw_dmclock_admin_lim"}),

    Option("rgw_dmclock_admin_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("mclock limit for admin requests")
    .set_default(0.0)
    .add_service("rgw")
    .add_see_also({"rgw_dmclock_admin_res", "rgw_dmclock_admin_wgt"}),

    Option("rgw_dmclock_auth_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("mclock reservation for object data requests")
    .set_default(200.0)
    .add_service("rgw")
    .add_see_also({"rgw_dmclock_auth_wgt", "rgw_dmclock_auth_lim"}),

    Option("rgw_dmclock_auth_wgt", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("mclock weight for object data requests")
    .set_default(100.0)
    .add_service("rgw")
    .add_see_also({"rgw_dmclock_auth_res", "rgw_dmclock_auth_lim"}),

    Option("rgw_dmclock_auth_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("mclock limit for object data requests")
    .set_default(0.0)
    .add_service("rgw")
    .add_see_also({"rgw_dmclock_auth_res", "rgw_dmclock_auth_wgt"}),

    Option("rgw_dmclock_data_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("mclock reservation for object data requests")
    .set_default(500.0)
    .add_service("rgw")
    .add_see_also({"rgw_dmclock_data_wgt", "rgw_dmclock_data_lim"}),

    Option("rgw_dmclock_data_wgt", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("mclock weight for object data requests")
    .set_default(500.0)
    .add_service("rgw")
    .add_see_also({"rgw_dmclock_data_res", "rgw_dmclock_data_lim"}),

    Option("rgw_dmclock_data_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("mclock limit for object data requests")
    .set_default(0.0)
    .add_service("rgw")
    .add_see_also({"rgw_dmclock_data_res", "rgw_dmclock_data_wgt"}),

    Option("rgw_dmclock_metadata_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("mclock reservation for metadata requests")
    .set_default(500.0)
    .add_service("rgw")
    .add_see_also({"rgw_dmclock_metadata_wgt", "rgw_dmclock_metadata_lim"}),

    Option("rgw_dmclock_metadata_wgt", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("mclock weight for metadata requests")
    .set_default(500.0)
    .add_service("rgw")
    .add_see_also({"rgw_dmclock_metadata_res", "rgw_dmclock_metadata_lim"}),

    Option("rgw_dmclock_metadata_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("mclock limit for metadata requests")
    .set_default(0.0)
    .add_service("rgw")
    .add_see_also({"rgw_dmclock_metadata_res", "rgw_dmclock_metadata_wgt"}),

    Option("rgw_default_data_log_backing", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Default backing store for the RGW data sync log")
    .set_long_description("Whether to use the older OMAP backing store or the high performance FIFO based backing store by default. This only covers the creation of the log on startup if none exists.")
    .set_default("fifo")
    .set_enum_allowed({"fifo", "omap"})
    .add_service("rgw"),

    Option("rgw_d3n_l1_local_datacache_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable datacenter-scale dataset delivery local cache")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_d3n_l1_datacache_persistent_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path for the directory for storing the local cache objects data")
    .set_default("/tmp/rgw_datacache/")
    .add_service("rgw"),

    Option("rgw_d3n_l1_datacache_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("datacache maximum size on disk in bytes")
    .set_default(1_G)
    .add_service("rgw"),

    Option("rgw_d3n_l1_evict_cache_on_start", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("clear the content of the persistent data cache directory on start")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_d3n_l1_fadvise", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("posix_fadvise() flag for access pattern of cache files")
    .set_long_description("for example to bypass the page-cache - POSIX_FADV_DONTNEED=4")
    .set_default(4)
    .add_service("rgw"),

    Option("rgw_d3n_l1_eviction_policy", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("select the d3n cache eviction policy")
    .set_default("lru")
    .set_enum_allowed({"lru", "random"})
    .add_service("rgw"),

    Option("rgw_d3n_libaio_aio_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("specifies the maximum number of worker threads that may be used by libaio")
    .set_default(20)
    .add_service("rgw")
    .add_see_also({"rgw_thread_pool_size"}),

    Option("rgw_d3n_libaio_aio_num", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("specifies the maximum number of simultaneous I/O requests that libaio expects to enqueue")
    .set_default(64)
    .add_service("rgw")
    .add_see_also({"rgw_thread_pool_size"}),

    Option("rgw_backend_store", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("experimental Option to set backend store type")
    .set_long_description("defaults to rados. Other valid values are dbstore, motr, and daos (All experimental).")
    .set_default("rados")
    .set_enum_allowed({"rados", "dbstore", "motr", "daos"})
    .add_service("rgw"),

    Option("rgw_config_store", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Configuration storage backend")
    .set_default("rados")
    .set_enum_allowed({"rados", "dbstore", "json"})
    .add_service("rgw"),

    Option("rgw_filter", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("experimental Option to set a filter")
    .set_long_description("defaults to none. Other valid values are base and d4n (both experimental).")
    .set_default("none")
    .set_enum_allowed({"none", "base", "d4n", "posix"})
    .add_service("rgw"),

    Option("dbstore_db_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path for the directory for storing the db backend store data")
    .set_default("/var/lib/ceph/radosgw")
    .add_service("rgw"),

    Option("dbstore_db_name_prefix", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("prefix to the file names created by db backend store")
    .set_default("dbstore")
    .add_service("rgw"),

    Option("dbstore_config_uri", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Config database URI. URIs beginning with file: refer to local files opened with SQLite.")
    .set_default("file:/var/lib/ceph/radosgw/dbstore-config.db")
    .add_service("rgw")
    .add_see_also({"rgw_config_store"}),

    Option("rgw_json_config", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Path to a json file that contains the static zone and zonegroup configuration. Requires rgw_config_store=json.")
    .set_default("/var/lib/ceph/radosgw/config.json")
    .add_service("rgw")
    .add_see_also({"rgw_config_store"}),

    Option("motr_profile_fid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("experimental Option to set Motr profile fid")
    .set_long_description("example value 0x7000000000000001:0x4f")
    .set_default("0x7000000000000001:0x0")
    .add_service("rgw"),

    Option("motr_my_fid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("experimental Option to set my Motr fid")
    .set_long_description("example value 0x7200000000000001:0x29")
    .set_default("0x7200000000000001:0x0")
    .add_service("rgw"),

    Option("motr_admin_fid", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Admin Tool Motr FID for admin-level access.")
    .set_long_description("example value 0x7200000000000001:0x2c")
    .set_default("0x7200000000000001:0x0")
    .add_service("rgw"),

    Option("motr_admin_endpoint", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("experimental Option to set Admin Motr endpoint address")
    .set_long_description("example value 192.168.180.182@tcp:12345:4:1")
    .set_default("192.168.180.182@tcp:12345:4:1")
    .add_service("rgw"),

    Option("motr_my_endpoint", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("experimental Option to set my Motr endpoint address")
    .set_long_description("example value 192.168.180.182@tcp:12345:4:1")
    .set_default("192.168.180.182@tcp:12345:4:1")
    .add_service("rgw"),

    Option("motr_ha_endpoint", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("experimental Option to set Motr HA agent endpoint address")
    .set_long_description("example value 192.168.180.182@tcp:12345:1:1")
    .set_default("192.168.180.182@tcp:12345:1:1")
    .add_service("rgw"),

    Option("motr_tracing_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Set to true when Motr client debugging is needed")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_posix_base_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("experimental Option to set base path for POSIX Driver")
    .set_long_description("Base path for the POSIX driver.  All operations are relative to this path. Defaults to /tmp/rgw_posix_driver")
    .set_default("/tmp/rgw_posix_driver")
    .add_service("rgw"),

    Option("rgw_posix_database_root", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("experimental Path to parent of POSIX Driver LMDB bucket listing cache")
    .set_long_description("Parent directory of LMDB bucket listing cache databases.")
    .set_default("/var/lib/ceph/radosgw")
    .add_service("rgw"),

    Option("rgw_posix_cache_max_buckets", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("experimental Number of buckets to maintain in the ordered listing cache")
    .set_default(100)
    .add_service("rgw"),

    Option("rgw_posix_cache_lanes", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("experimental Number of lanes in cache LRU")
    .set_default(3)
    .add_service("rgw"),

    Option("rgw_posix_cache_partitions", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("experimental Number of partitions in cache AVL")
    .set_default(3)
    .add_service("rgw"),

    Option("rgw_posix_cache_lmdb_count", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("experimental Number of lmdb partitions in the ordered listing cache")
    .set_default(3)
    .add_service("rgw"),

    Option("rgw_luarocks_location", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Directory where luarocks install packages from allowlist")
    .set_default("/tmp/rgw_luarocks/$name")
    .set_flag(Option::FLAG_STARTUP)
    .add_service("rgw"),

    Option("rgwlc_auto_session_clear", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Automatically clear stale lifecycle sessions (i.e., after 2 idle processing cycles)")
    .set_default(true)
    .add_service("rgw"),

    Option("rgwlc_skip_bucket_step", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Conditionally skip the processing (but not the scheduling) of bucket lifecycle")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_pending_bucket_index_op_expiration", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of seconds a pending operation can remain in bucket index shard.")
    .set_long_description("Number of seconds a pending operation can remain in bucket index shard before it expires. Used for transactional bucket index operations, and if the operation does not complete in this time period, the operation will be dropped.")
    .set_default(120)
    .add_service({"rgw", "osd"}),

    Option("rgw_bucket_index_transaction_instrumentation", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Turns on extra instrumentation surrounding bucket index transactions.")
    .set_default(false)
    .add_service({"rgw", "osd"}),

    Option("rgw_allow_notification_secrets_in_cleartext", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Allows sending secrets (e.g. passwords) over non encrypted HTTP messages.")
    .set_long_description("When bucket notification endpoint require secrets (e.g. passwords), we allow the topic creation only over HTTPS messages. This parameter can be set to \"true\" to bypass this check. Use this only if radosgw is on a trusted private network, and the message broker cannot be configured without password authentication. Otherwise, this will leak the credentials of your message broker and compromise its security.")
    .set_default(false)
    .add_service("rgw")
    .add_see_also({"rgw_trust_forwarded_https"}),

    Option("daos_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("DAOS Pool to use")
    .set_default("tank")
    .add_service("rgw"),

    Option("rgw_policy_reject_invalid_principals", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("Whether to reject policies with invalid principals")
    .set_long_description("If true, policies with invalid principals will be rejected. We don't support Canonical User identifiers or some other form of policies that Amazon does, so if you are mirroring policies between RGW and AWS, you may wish to set this to false.")
    .set_default(true)
    .add_service("rgw"),

    Option("rgw_d4n_host", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("The rgw directory host")
    .set_default("127.0.0.1")
    .set_flag(Option::FLAG_STARTUP)
    .add_service("rgw"),

    Option("rgw_d4n_port", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("The rgw directory port")
    .set_default(6379)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("rgw"),

    Option("rgw_topic_persistency_time_to_live", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("The rgw retention of persistent topics by time (seconds)")
    .set_default(0)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("rgw"),

    Option("rgw_topic_persistency_max_retries", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("The maximum number sending a persistent notification would be tried. Note that the value of one would mean no retries, and the value of zero would mean that the notification would be tried indefinitely")
    .set_default(0)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("rgw"),

    Option("rgw_topic_persistency_sleep_duration", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("The minimum time (in seconds) between two tries of the same persistent notification. note that the actual time between the tries may be longer")
    .set_default(0)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("rgw"),

    Option("rgw_lua_max_memory_per_state", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Max size of memory used by a single lua state")
    .set_long_description("This is the maximum size in bytes that a lua state can allocate for its own use. Note that this does not include any memory that can be accessed from lua, but managed by the RGW. If not set, it would use a default of 128K. If set to zero, the amount of memory would only be limited by the system.")
    .set_default(128000)
    .add_service("rgw"),

    Option("mandatory_topic_permissions", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("Whether to validate user permissions to access notification topics.")
    .set_long_description("If true, all users (other then the owner of the topic) will need to have a policy to access topics. The topic policy can be set by owner via CreateTopic() or SetTopicAttribute(). Following permissions can be granted \"sns:Publish\", \"sns:GetTopicAttributes\", \"sns:SetTopicAttributes\" and \"sns:DeleteTopic\" via Policy. NOTE that even if set to \"false\" topics will still follow the policies if set on them.")
    .set_default(false)
    .add_service("rgw"),

    Option("rgw_user_counters_cache", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("enable a rgw perf counters cache for counters with user label")
    .set_default(false)
    .add_service("rgw")
    .add_see_also({"rgw_user_counters_cache_size"}),

    Option("rgw_user_counters_cache_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of labeled perf counters the user perf counters cache can store")
    .set_default(10000)
    .add_service("rgw")
    .add_see_also({"rgw_user_counters_cache"}),

    Option("rgw_bucket_counters_cache", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("enable a rgw perf counters cache for counters with bucket label")
    .set_default(false)
    .add_service("rgw")
    .add_see_also({"rgw_bucket_counters_cache_size"}),

    Option("rgw_bucket_counters_cache_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of labeled perf counters the bucket perf counters cache can store")
    .set_default(10000)
    .add_service("rgw")
    .add_see_also({"rgw_bucket_counters_cache"}),


  });
}
