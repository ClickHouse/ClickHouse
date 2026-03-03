-- ClickHouse Map and JSON type dictionary test script
--
-- This script contains the following tests:
-- 1. Map type dictionary tests (FLAT and HASHED layouts)
-- 2. JSON type dictionary tests (FLAT and HASHED layouts)
-- 3. Complex type field access tests
-- 4. Batch query tests

-- 1. Create test data source table
CREATE TABLE IF NOT EXISTS map_test_source
(
    id UInt64,
    name String,
    metadata Map(String, String),
    tags Map(String, Array(String)),
    scores Map(String, Float64)
)
ENGINE = Memory;

-- 2. Insert test data
INSERT INTO map_test_source VALUES
(1, 'Alice', {'age': '30', 'city': 'New York', 'country': 'USA'}, {'hobbies': ['reading', 'swimming'], 'skills': ['python', 'sql']}, {'math': 95.5, 'english': 88.0}),
(2, 'Bob', {'age': '25', 'city': 'London', 'country': 'UK'}, {'hobbies': ['gaming', 'music'], 'skills': ['java', 'javascript']}, {'math': 87.5, 'english': 92.0}),
(3, 'Charlie', {'age': '35', 'city': 'Tokyo', 'country': 'Japan'}, {'hobbies': ['cooking', 'travel'], 'skills': ['c++', 'go']}, {'math': 90.0, 'english': 85.5});

-- 3. Create FLAT dictionary (test Map type)
CREATE DICTIONARY IF NOT EXISTS map_test_flat
(
    id UInt64,
    name String,
    metadata Map(String, String),
    tags Map(String, Array(String)),
    scores Map(String, Float64)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB 'default'
    TABLE 'map_test_source'
))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 0);

-- 4. Create HASHED dictionary (test Map type)
CREATE DICTIONARY IF NOT EXISTS map_test_hashed
(
    id UInt64,
    name String,
    metadata Map(String, String),
    tags Map(String, Array(String)),
    scores Map(String, Float64)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB 'default'
    TABLE 'map_test_source'
))
LAYOUT(HASHED())
LIFETIME(MIN 0 MAX 0);

-- 5. Test query - FLAT dictionary
SELECT '=== Testing FLAT Dictionary ===' as test;
SELECT
    id,
    dictGet('map_test_flat', 'name', id) as name,
    dictGet('map_test_flat', 'metadata', id) as metadata,
    dictGet('map_test_flat', 'tags', id) as tags,
    dictGet('map_test_flat', 'scores', id) as scores
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 6. Test query - HASHED dictionary
SELECT '=== Testing HASHED Dictionary ===' as test;
SELECT
    id,
    dictGet('map_test_hashed', 'name', id) as name,
    dictGet('map_test_hashed', 'metadata', id) as metadata,
    dictGet('map_test_hashed', 'tags', id) as tags,
    dictGet('map_test_hashed', 'scores', id) as scores
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 7. Test Map key-value access
SELECT '=== Testing Map Key Access ===' as test;
SELECT
    id,
    dictGet('map_test_flat', 'name', id) as name,
    dictGet('map_test_flat', 'metadata', id)['city'] as city,
    dictGet('map_test_flat', 'metadata', id)['age'] as age,
    dictGet('map_test_flat', 'scores', id)['math'] as math_score
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 8. Test nested Map (Map with Array values)
SELECT '=== Testing Nested Map ===' as test;
SELECT
    id,
    dictGet('map_test_flat', 'name', id) as name,
    dictGet('map_test_flat', 'tags', id)['hobbies'] as hobbies,
    dictGet('map_test_flat', 'tags', id)['skills'] as skills
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 9. Test dictionary getColumn method (batch query)
SELECT '=== Testing getColumn (Batch Query) ===' as test;
SELECT
    number as id,
    dictGet('map_test_flat', 'metadata', number) as metadata
FROM numbers(1, 3);

-- 10. Verify dictionary info
SELECT '=== Dictionary Info ===' as test;
SELECT name, type, key, attribute.names, attribute.types
FROM system.dictionaries
WHERE name LIKE 'map_test%';

-- ============================================
-- JSON type dictionary tests
-- ============================================

-- 11. Create test data source table with JSON type
CREATE TABLE IF NOT EXISTS json_test_source
(
    id UInt64,
    name String,
    profile JSON,
    settings JSON,
    preferences JSON,
    metadata JSON
)
ENGINE = Memory;

-- 12. Insert JSON test data
INSERT INTO json_test_source VALUES
(1, 'Alice', '{"age": 30, "city": "New York", "country": "USA", "hobbies": ["reading", "swimming"], "skills": {"programming": ["python", "sql"], "languages": ["english", "spanish"]}}', '{"theme": "dark", "notifications": true, "auto_save": false}', '{"display": {"font_size": 14, "color_scheme": "blue"}, "privacy": {"share_data": false}}', '{"created_at": "2024-01-15", "last_login": "2024-12-01", "status": "active"}'),
(2, 'Bob', '{"age": 25, "city": "London", "country": "UK", "hobbies": ["gaming", "music"], "skills": {"programming": ["java", "javascript"], "languages": ["english", "french"]}}', '{"theme": "light", "notifications": false, "auto_save": true}', '{"display": {"font_size": 12, "color_scheme": "green"}, "privacy": {"share_data": true}}', '{"created_at": "2024-02-20", "last_login": "2024-11-28", "status": "active"}'),
(3, 'Charlie', '{"age": 35, "city": "Tokyo", "country": "Japan", "hobbies": ["cooking", "travel"], "skills": {"programming": ["c++", "go"], "languages": ["japanese", "english"]}}', '{"theme": "dark", "notifications": true, "auto_save": true}', '{"display": {"font_size": 16, "color_scheme": "red"}, "privacy": {"share_data": false}}', '{"created_at": "2024-03-10", "last_login": "2024-12-02", "status": "inactive"}');

-- 13. Create FLAT dictionary (test JSON type)
CREATE DICTIONARY IF NOT EXISTS json_test_flat
(
    id UInt64,
    name String,
    profile JSON,
    settings JSON,
    preferences JSON,
    metadata JSON
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB 'default'
    TABLE 'json_test_source'
))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 0);

-- 14. Create HASHED dictionary (test JSON type)
CREATE DICTIONARY IF NOT EXISTS json_test_hashed
(
    id UInt64,
    name String,
    profile JSON,
    settings JSON,
    preferences JSON,
    metadata JSON
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB 'default'
    TABLE 'json_test_source'
))
LAYOUT(HASHED())
LIFETIME(MIN 0 MAX 0);

-- 15. Test query - FLAT dictionary (JSON type)
SELECT '=== Testing FLAT Dictionary (JSON/Object) ===' as test;
SELECT
    id,
    dictGet('json_test_flat', 'name', id) as name,
    dictGet('json_test_flat', 'profile', id) as profile,
    dictGet('json_test_flat', 'settings', id) as settings,
    dictGet('json_test_flat', 'preferences', id) as preferences,
    dictGet('json_test_flat', 'metadata', id) as metadata
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 16. Test query - HASHED dictionary (JSON type)
SELECT '=== Testing HASHED Dictionary (JSON/Object) ===' as test;
SELECT
    id,
    dictGet('json_test_hashed', 'name', id) as name,
    dictGet('json_test_hashed', 'profile', id) as profile,
    dictGet('json_test_hashed', 'settings', id) as settings,
    dictGet('json_test_hashed', 'preferences', id) as preferences,
    dictGet('json_test_hashed', 'metadata', id) as metadata
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 17. Test JSON field access (using JSON sub-column access)
SELECT '=== Testing JSON Field Access ===' as test;
SELECT
    id,
    dictGet('json_test_flat', 'name', id) as name,
    dictGet('json_test_flat', 'profile', id).age as age,
    dictGet('json_test_flat', 'profile', id).city as city,
    dictGet('json_test_flat', 'profile', id).country as country,
    dictGet('json_test_flat', 'settings', id).theme as theme,
    dictGet('json_test_flat', 'metadata', id).status as status
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 18. Test nested JSON access
SELECT '=== Testing Nested JSON Access ===' as test;
SELECT
    id,
    dictGet('json_test_flat', 'name', id) as name,
    dictGet('json_test_flat', 'profile', id).hobbies as hobbies,
    dictGet('json_test_flat', 'profile', id).skills as skills,
    dictGet('json_test_flat', 'preferences', id).display as display,
    dictGet('json_test_flat', 'preferences', id).privacy as privacy
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 19. Test JSON array access
SELECT '=== Testing JSON Array Access ===' as test;
SELECT
    id,
    dictGet('json_test_flat', 'name', id) as name,
    dictGet('json_test_flat', 'profile', id).hobbies as hobbies,
    dictGet('json_test_flat', 'profile', id).hobbies[1] as first_hobby,
    dictGet('json_test_flat', 'profile', id).skills.programming as programming_skills
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 20. Test dictionary getColumn method (batch query JSON)
SELECT '=== Testing getColumn (Batch Query JSON/Object) ===' as test;
SELECT
    number as id,
    dictGet('json_test_flat', 'profile', number) as profile,
    dictGet('json_test_flat', 'settings', number) as settings
FROM numbers(1, 3);

-- 21. Test multiple JSON fields usage
SELECT '=== Testing Multiple JSON Fields ===' as test;
SELECT
    id,
    dictGet('json_test_flat', 'name', id) as name,
    dictGet('json_test_flat', 'profile', id) as profile,
    dictGet('json_test_flat', 'settings', id) as settings,
    dictGet('json_test_flat', 'preferences', id) as preferences,
    dictGet('json_test_flat', 'metadata', id) as metadata
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 22. Verify JSON dictionary info
SELECT '=== JSON/Object Dictionary Info ===' as test;
SELECT name, type, key, attribute.names, attribute.types
FROM system.dictionaries
WHERE name LIKE 'json_test%';

-- 23. Combined test: using both Map and JSON types
SELECT '=== Combined Test: Map and JSON/Object ===' as test;
SELECT
    m.id,
    dictGet('map_test_flat', 'name', m.id) as map_name,
    dictGet('map_test_flat', 'metadata', m.id)['city'] as map_city,
    dictGet('json_test_flat', 'name', m.id) as json_name,
    dictGet('json_test_flat', 'profile', m.id).city as json_city
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3) as m;

-- ============================================
-- Deep nested JSON type dictionary tests
-- ============================================

-- 24. Create test data source table with deep nested JSON type
CREATE TABLE IF NOT EXISTS deep_json_test_source
(
    id UInt64,
    name String,
    deep_data JSON,
    nested_structure JSON,
    complex_hierarchy JSON
)
ENGINE = Memory;

-- 25. Insert deep nested JSON test data
INSERT INTO deep_json_test_source VALUES
(1, 'DeepTest1', '{"level1":{"level2":{"level3":{"level4":{"level5":{"value":"deep_value_1","numbers":[1,2,3],"nested":{"key":"nested_value_1"}}}}}}}', '{"company":{"departments":[{"name":"Engineering","teams":[{"name":"Backend","members":[{"name":"Alice","role":"Senior","projects":[{"name":"Project A","status":"active","details":{"start_date":"2024-01-01","budget":100000}}]}]}]},{"name":"Product","teams":[{"name":"Design","members":[{"name":"Bob","role":"Lead","projects":[{"name":"Project B","status":"planning"}]}]}]}]}}', '{"user":{"profile":{"personal":{"name":"Charlie","age":30,"address":{"street":"123 Main St","city":"New York","country":{"code":"US","name":"United States","regions":[{"name":"Northeast","states":[{"name":"NY","cities":[{"name":"NYC","boroughs":["Manhattan","Brooklyn"]}]}]}]}}},"work":{"company":"Tech Corp","position":"Engineer","projects":[{"id":1,"name":"Alpha","team":{"lead":"David","members":["Eve","Frank"]}}]}}}}');

INSERT INTO deep_json_test_source VALUES
(2, 'DeepTest2', '{"level1":{"level2":{"level3":{"level4":{"level5":{"level6":{"value":"very_deep_value","array":[{"item":"a","sub":{"data":"x"}},{"item":"b","sub":{"data":"y"}}]}}}}}}}', '{"organization":{"divisions":{"tech":{"teams":{"frontend":{"members":[{"id":1,"name":"Alice","skills":{"languages":["JS","TS"],"frameworks":{"react":{"version":"18","projects":[{"name":"App1","features":["feature1","feature2"]}]}}}}]},"backend":{"members":[{"id":2,"name":"Bob","skills":{"languages":["Python","Go"],"databases":{"primary":"PostgreSQL","cache":"Redis"}}}]}}}}}}', '{"system":{"config":{"database":{"connections":{"primary":{"host":"db1.example.com","port":5432,"replicas":[{"host":"db1-replica1.example.com","port":5432,"region":"us-east-1"},{"host":"db1-replica2.example.com","port":5432,"region":"us-west-2"}]},"secondary":{"host":"db2.example.com","port":5432}}},"cache":{"redis":{"cluster":{"nodes":[{"host":"redis1","port":6379,"slots":[0,5460]},{"host":"redis2","port":6379,"slots":[5461,10922]}]}}}}}}');

INSERT INTO deep_json_test_source VALUES
(3, 'DeepTest3', '{"a":{"b":{"c":{"d":{"e":{"f":{"value":"extremely_deep","metadata":{"created":"2024-01-01","tags":["tag1","tag2"],"nested_obj":{"key1":"val1","key2":"val2"}}}}}}}}}', '{"root":{"branch1":{"leaf1":{"data":"value1","children":[{"id":1,"name":"child1","attributes":{"color":"red","size":"large","details":{"material":"wood","origin":{"country":"USA","state":"CA"}}}},{"id":2,"name":"child2","attributes":{"color":"blue","size":"medium"}}]},"leaf2":{"data":"value2","children":[{"id":3,"name":"child3"}]}},"branch2":{"leaf3":{"data":"value3"}}}}', '{"application":{"modules":{"auth":{"providers":[{"type":"oauth2","config":{"client_id":"abc123","endpoints":{"authorization":"https://auth.example.com/authorize","token":"https://auth.example.com/token","userinfo":{"url":"https://auth.example.com/userinfo","headers":{"Authorization":"Bearer token"}}}}},{"type":"saml","config":{"entity_id":"saml-entity","sso_url":"https://saml.example.com/sso"}}]},"api":{"version":"v1","endpoints":[{"path":"/users","methods":["GET","POST"],"middleware":["auth","logging"],"handlers":{"GET":{"function":"getUsers","params":{"limit":100,"offset":0}}}}]}}}}');

-- 26. Create FLAT dictionary (test deep nested JSON type)
CREATE DICTIONARY IF NOT EXISTS deep_json_test_flat
(
    id UInt64,
    name String,
    deep_data JSON,
    nested_structure JSON,
    complex_hierarchy JSON
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB 'default'
    TABLE 'deep_json_test_source'
))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 0);

-- 27. Create HASHED dictionary (test deep nested JSON type)
CREATE DICTIONARY IF NOT EXISTS deep_json_test_hashed
(
    id UInt64,
    name String,
    deep_data JSON,
    nested_structure JSON,
    complex_hierarchy JSON
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB 'default'
    TABLE 'deep_json_test_source'
))
LAYOUT(HASHED())
LIFETIME(MIN 0 MAX 0);

-- 28. Test deep nested JSON access (5 and 6 levels deep)
SELECT '=== Testing Deep Nested JSON (Multiple Depths) ===' as test;
SELECT
    id,
    dictGet('deep_json_test_flat', 'name', id) as name,
    -- Test 5 levels deep (id=1)
    dictGet('deep_json_test_flat', 'deep_data', id).level1.level2.level3.level4.level5.value as deep_value_5,
    dictGet('deep_json_test_flat', 'deep_data', id).level1.level2.level3.level4.level5.numbers as deep_numbers,
    dictGet('deep_json_test_flat', 'deep_data', id).level1.level2.level3.level4.level5.nested.key as nested_key,
    -- Test 6 levels deep (id=2)
    dictGet('deep_json_test_flat', 'deep_data', id).level1.level2.level3.level4.level5.level6.value as deep_value_6,
    dictGet('deep_json_test_flat', 'deep_data', id).level1.level2.level3.level4.level5.level6.array as deep_array,
    -- Test 6 levels deep with metadata (id=3)
    dictGet('deep_json_test_flat', 'deep_data', id).a.b.c.d.e.f.value as deep_value_6_alt,
    dictGet('deep_json_test_flat', 'deep_data', id).a.b.c.d.e.f.metadata.tags as deep_tags,
    dictGet('deep_json_test_flat', 'deep_data', id).a.b.c.d.e.f.metadata.nested_obj.key2 as nested_key2
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 29. Test deep nested JSON with arrays and objects
SELECT '=== Testing Deep Nested JSON with Arrays and Objects ===' as test;
SELECT
    id,
    dictGet('deep_json_test_flat', 'name', id) as name,
    dictGet('deep_json_test_flat', 'nested_structure', id).company.departments as departments,
    dictGet('deep_json_test_flat', 'nested_structure', id).company.departments[1].name as dept_name,
    dictGet('deep_json_test_flat', 'nested_structure', id).company.departments[1].teams[1].name as team_name,
    dictGet('deep_json_test_flat', 'nested_structure', id).company.departments[1].teams[1].members[1].name as member_name,
    dictGet('deep_json_test_flat', 'nested_structure', id).company.departments[1].teams[1].members[1].projects[1].name as project_name,
    dictGet('deep_json_test_flat', 'nested_structure', id).company.departments[1].teams[1].members[1].projects[1].details.start_date as project_start_date
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 30. Test complex hierarchy with multiple nested levels
SELECT '=== Testing Complex Hierarchy (Multiple Nested Levels) ===' as test;
SELECT
    id,
    dictGet('deep_json_test_flat', 'name', id) as name,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).user.profile.personal.name as user_name,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).user.profile.personal.address.city as city,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).user.profile.personal.address.country.name as country_name,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).user.profile.personal.address.country.regions[1].name as region_name,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).user.profile.personal.address.country.regions[1].states[1].name as state_name,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).user.profile.personal.address.country.regions[1].states[1].cities[1].name as city_name,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).user.profile.personal.address.country.regions[1].states[1].cities[1].boroughs[1] as borough_name
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 31. Test nested arrays within nested objects
SELECT '=== Testing Nested Arrays within Nested Objects ===' as test;
SELECT
    id,
    dictGet('deep_json_test_flat', 'name', id) as name,
    dictGet('deep_json_test_flat', 'nested_structure', id).organization.divisions.tech.teams.frontend.members[1].skills.languages as languages,
    dictGet('deep_json_test_flat', 'nested_structure', id).organization.divisions.tech.teams.frontend.members[1].skills.frameworks.react.version as react_version,
    dictGet('deep_json_test_flat', 'nested_structure', id).organization.divisions.tech.teams.frontend.members[1].skills.frameworks.react.projects[1].name as project_name,
    dictGet('deep_json_test_flat', 'nested_structure', id).organization.divisions.tech.teams.frontend.members[1].skills.frameworks.react.projects[1].features[1] as feature_name
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 32. Test deeply nested complex hierarchy structures (system config and application modules)
SELECT '=== Testing Deeply Nested Complex Hierarchy Structures ===' as test;
SELECT
    id,
    dictGet('deep_json_test_flat', 'name', id) as name,
    -- Test system configuration structure (id=2)
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).system.config.database.connections.primary.host as primary_host,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).system.config.database.connections.primary.replicas[1].host as replica_host,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).system.config.database.connections.primary.replicas[1].region as replica_region,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).system.config.cache.redis.cluster.nodes[1].host as redis_host,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).system.config.cache.redis.cluster.nodes[1].slots[1] as redis_slot,
    -- Test application modules structure (id=3)
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).application.modules.auth.providers[1].type as provider_type,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).application.modules.auth.providers[1].config.client_id as client_id,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).application.modules.auth.providers[1].config.endpoints.userinfo.url as userinfo_url,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).application.modules.auth.providers[1].config.endpoints.userinfo.headers.Authorization as auth_header,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).application.modules.api.endpoints[1].path as api_path,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).application.modules.api.endpoints[1].handlers.GET.function as handler_function
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 33. Test HASHED dictionary with deep nested JSON
SELECT '=== Testing HASHED Dictionary with Deep Nested JSON ===' as test;
SELECT
    id,
    dictGet('deep_json_test_hashed', 'name', id) as name,
    dictGet('deep_json_test_hashed', 'deep_data', id).level1.level2.level3.level4.level5.value as deep_value,
    dictGet('deep_json_test_hashed', 'nested_structure', id).company.departments[1].teams[1].members[1].name as member_name,
    dictGet('deep_json_test_hashed', 'complex_hierarchy', id).user.profile.personal.address.country.name as country_name
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3);

-- 34. Test batch query with deep nested JSON
SELECT '=== Testing Batch Query with Deep Nested JSON ===' as test;
SELECT
    number as id,
    dictGet('deep_json_test_flat', 'name', number) as name,
    dictGet('deep_json_test_flat', 'deep_data', number) as deep_data,
    dictGet('deep_json_test_flat', 'nested_structure', number) as nested_structure
FROM numbers(1, 3);

-- 35. Verify deep JSON dictionary info
SELECT '=== Deep JSON Dictionary Info ===' as test;
SELECT name, type, key, attribute.names, attribute.types
FROM system.dictionaries
WHERE name LIKE 'deep_json_test%';
