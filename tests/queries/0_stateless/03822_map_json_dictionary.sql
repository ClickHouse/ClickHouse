-- ClickHouse dictionary tests for `Map` and `JSON`.
--
-- Coverage overview:
-- - Unified source with `Map` + `JSON` attributes across layouts:
--   `FLAT`, `HASHED`, `HASHED_ARRAY`, `CACHE`, `RANGE_HASHED`
-- - Missing key materialization (`dictGet`, `dictGetOrDefault`)
-- - Typed JSON access via `CAST` and `toTypeName`
-- - Nested/array/sub-object JSON access and JSON introspection helpers
-- - Regression for dictionary `JSON(path TypeName, SKIP REGEXP ...)`
-- - Deep nested JSON, `Nullable(JSON)`, and `IP_TRIE` scenarios

-- Section A. Unified source table for `Map` + `JSON` layout tests
CREATE TABLE IF NOT EXISTS mixed_test_source
(
    id UInt64,
    start_date Date,
    end_date Date,
    name String,
    metadata Map(String, String),
    tags Map(String, Array(String)),
    scores Map(String, Float64),
    profile JSON,
    settings JSON,
    preferences JSON,
    metadata_json JSON
)
ENGINE = Memory;

-- Section B. Insert unified test data
INSERT INTO mixed_test_source VALUES
(
    1,
    toDate('2024-01-01'),
    toDate('2024-01-31'),
    'Alice',
    {'age': '30', 'city': 'New York', 'country': 'USA'},
    {'hobbies': ['reading', 'swimming'], 'skills': ['python', 'sql']},
    {'math': 95.5, 'english': 88.0},
    '{"age": 30, "city": "New York", "country": "USA", "hobbies": ["reading", "swimming"], "skills": {"programming": ["python", "sql"], "languages": ["english", "spanish"]}}',
    '{"theme": "dark", "notifications": true, "auto_save": false}',
    '{"display": {"font_size": 14, "color_scheme": "blue"}, "privacy": {"share_data": false}}',
    '{"created_at": "2024-01-15", "last_login": "2024-12-01", "status": "active"}'
),
(
    2,
    toDate('2024-02-01'),
    toDate('2024-02-28'),
    'Bob',
    {'age': '25', 'city': 'London', 'country': 'UK'},
    {'hobbies': ['gaming', 'music'], 'skills': ['java', 'javascript']},
    {'math': 87.5, 'english': 92.0},
    '{"age": 25, "city": "London", "country": "UK", "hobbies": ["gaming", "music"], "skills": {"programming": ["java", "javascript"], "languages": ["english", "french"]}}',
    '{"theme": "light", "notifications": false, "auto_save": true}',
    '{"display": {"font_size": 12, "color_scheme": "green"}, "privacy": {"share_data": true}}',
    '{"created_at": "2024-02-20", "last_login": "2024-11-28", "status": "active"}'
),
(
    3,
    toDate('2024-03-01'),
    toDate('2024-03-31'),
    'Charlie',
    {'age': '35', 'city': 'Tokyo', 'country': 'Japan'},
    {'hobbies': ['cooking', 'travel'], 'skills': ['c++', 'go']},
    {'math': 90.0, 'english': 85.5},
    '{"age": 35, "city": "Tokyo", "country": "Japan", "hobbies": ["cooking", "travel"], "skills": {"programming": ["c++", "go"], "languages": ["japanese", "english"]}}',
    '{"theme": "dark", "notifications": true, "auto_save": true}',
    '{"display": {"font_size": 16, "color_scheme": "red"}, "privacy": {"share_data": false}}',
    '{"created_at": "2024-03-10", "last_login": "2024-12-02", "status": "inactive"}'
);

-- Section C. Dictionaries from unified source table
CREATE DICTIONARY IF NOT EXISTS mixed_test_flat
(
    id UInt64,
    name String,
    metadata Map(String, String),
    tags Map(String, Array(String)),
    scores Map(String, Float64),
    profile JSON,
    settings JSON,
    preferences JSON,
    metadata_json JSON
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB currentDatabase()
    TABLE 'mixed_test_source'
))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 0);

CREATE DICTIONARY IF NOT EXISTS mixed_test_hashed
(
    id UInt64,
    name String,
    metadata Map(String, String),
    tags Map(String, Array(String)),
    scores Map(String, Float64),
    profile JSON,
    settings JSON,
    preferences JSON,
    metadata_json JSON
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB currentDatabase()
    TABLE 'mixed_test_source'
))
LAYOUT(HASHED())
LIFETIME(MIN 0 MAX 0);

CREATE DICTIONARY IF NOT EXISTS mixed_test_hashed_array
(
    id UInt64,
    name String,
    metadata Map(String, String),
    tags Map(String, Array(String)),
    scores Map(String, Float64),
    profile JSON,
    settings JSON,
    preferences JSON,
    metadata_json JSON
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB currentDatabase()
    TABLE 'mixed_test_source'
))
LAYOUT(HASHED_ARRAY())
LIFETIME(MIN 0 MAX 0);

CREATE DICTIONARY IF NOT EXISTS mixed_test_cache
(
    id UInt64,
    name String,
    metadata Map(String, String),
    tags Map(String, Array(String)),
    scores Map(String, Float64),
    profile JSON,
    settings JSON,
    preferences JSON,
    metadata_json JSON
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB currentDatabase()
    TABLE 'mixed_test_source'
))
LAYOUT(CACHE(SIZE_IN_CELLS 1024))
LIFETIME(MIN 0 MAX 0);

CREATE DICTIONARY IF NOT EXISTS mixed_test_range_hashed
(
    id UInt64,
    start_date Date,
    end_date Date,
    name String,
    metadata Map(String, String),
    tags Map(String, Array(String)),
    scores Map(String, Float64),
    profile JSON,
    settings JSON,
    preferences JSON,
    metadata_json JSON
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB currentDatabase()
    TABLE 'mixed_test_source'
))
LAYOUT(RANGE_HASHED())
LIFETIME(MIN 0 MAX 0)
RANGE(MIN start_date MAX end_date);

-- Section D. One query per layout validating `Map` + `JSON` together
SELECT '=== Testing FLAT Layout (Map + JSON) ===' as test;
SELECT
    id,
    dictGet('mixed_test_flat', 'name', id) AS name,
    dictGet('mixed_test_flat', 'metadata', id)['city'] AS map_city,
    dictGet('mixed_test_flat', 'scores', id)['math'] AS map_math,
    dictGet('mixed_test_flat', 'profile', id).city AS json_city,
    dictGet('mixed_test_flat', 'settings', id).theme AS json_theme,
    dictGetOrDefault(
        'mixed_test_flat',
        'metadata',
        999,
        map('city', 'Unknown', 'age', '0')
    )['city'] AS missing_map_city,
    dictGetOrDefault(
        'mixed_test_flat',
        'metadata',
        999,
        map('city', 'Unknown', 'age', '0')
    )['age'] AS missing_map_age,
    dictGetOrDefault(
        'mixed_test_flat',
        'profile',
        999,
        '{"city":"Unknown","age":0}'::JSON
    ).city AS missing_json_city,
    dictGetOrDefault(
        'mixed_test_flat',
        'profile',
        999,
        '{"city":"Unknown","age":0}'::JSON
    ).age AS missing_json_age
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3)
ORDER BY id;

SELECT '=== Testing HASHED Layout (Map + JSON) ===' as test;
SELECT
    id,
    dictGet('mixed_test_hashed', 'name', id) AS name,
    dictGet('mixed_test_hashed', 'metadata', id)['city'] AS map_city,
    dictGet('mixed_test_hashed', 'scores', id)['math'] AS map_math,
    dictGet('mixed_test_hashed', 'profile', id).city AS json_city,
    dictGet('mixed_test_hashed', 'settings', id).theme AS json_theme,
    dictGetOrDefault('mixed_test_hashed', 'metadata', 999, map('city', 'Unknown', 'age', '0'))['city'] AS missing_map_city,
    dictGetOrDefault('mixed_test_hashed', 'metadata', 999, map('city', 'Unknown', 'age', '0'))['age'] AS missing_map_age,
    dictGetOrDefault('mixed_test_hashed', 'profile', 999, '{"city":"Unknown","age":0}'::JSON).city AS missing_json_city,
    dictGetOrDefault('mixed_test_hashed', 'profile', 999, '{"city":"Unknown","age":0}'::JSON).age AS missing_json_age
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3)
ORDER BY id;

SELECT '=== Testing HASHED_ARRAY Layout (Map + JSON) ===' as test;
SELECT
    id,
    dictGet('mixed_test_hashed_array', 'name', id) AS name,
    dictGet('mixed_test_hashed_array', 'metadata', id)['city'] AS map_city,
    dictGet('mixed_test_hashed_array', 'scores', id)['math'] AS map_math,
    dictGet('mixed_test_hashed_array', 'profile', id).city AS json_city,
    dictGet('mixed_test_hashed_array', 'settings', id).theme AS json_theme,
    dictGetOrDefault('mixed_test_hashed_array', 'metadata', 999, map('city', 'Unknown', 'age', '0'))['city'] AS missing_map_city,
    dictGetOrDefault('mixed_test_hashed_array', 'metadata', 999, map('city', 'Unknown', 'age', '0'))['age'] AS missing_map_age,
    dictGetOrDefault('mixed_test_hashed_array', 'profile', 999, '{"city":"Unknown","age":0}'::JSON).city AS missing_json_city,
    dictGetOrDefault('mixed_test_hashed_array', 'profile', 999, '{"city":"Unknown","age":0}'::JSON).age AS missing_json_age
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3)
ORDER BY id;

SELECT '=== Testing CACHE Layout (Map + JSON) ===' as test;
SELECT
    id,
    dictGet('mixed_test_cache', 'name', id) AS name,
    dictGet('mixed_test_cache', 'metadata', id)['city'] AS map_city,
    dictGet('mixed_test_cache', 'scores', id)['math'] AS map_math,
    dictGet('mixed_test_cache', 'profile', id).city AS json_city,
    dictGet('mixed_test_cache', 'settings', id).theme AS json_theme,
    dictGetOrDefault('mixed_test_cache', 'metadata', 999, map('city', 'Unknown', 'age', '0'))['city'] AS missing_map_city,
    dictGetOrDefault('mixed_test_cache', 'metadata', 999, map('city', 'Unknown', 'age', '0'))['age'] AS missing_map_age,
    dictGetOrDefault('mixed_test_cache', 'profile', 999, '{"city":"Unknown","age":0}'::JSON).city AS missing_json_city,
    dictGetOrDefault('mixed_test_cache', 'profile', 999, '{"city":"Unknown","age":0}'::JSON).age AS missing_json_age
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3)
ORDER BY id;

SELECT '=== Testing RANGE_HASHED Layout (Map + JSON) ===' as test;
SELECT
    id,
    dictGet('mixed_test_range_hashed', 'name', id, dt) AS name,
    dictGet('mixed_test_range_hashed', 'metadata', id, dt)['city'] AS map_city,
    dictGet('mixed_test_range_hashed', 'scores', id, dt)['math'] AS map_math,
    dictGet('mixed_test_range_hashed', 'profile', id, dt).city AS json_city,
    dictGet('mixed_test_range_hashed', 'settings', id, dt).theme AS json_theme,
    dictGetOrDefault(
        'mixed_test_range_hashed',
        'metadata',
        999,
        dt,
        map('city', 'Unknown', 'age', '0')
    )['city'] AS missing_map_city,
    dictGetOrDefault(
        'mixed_test_range_hashed',
        'metadata',
        999,
        dt,
        map('city', 'Unknown', 'age', '0')
    )['age'] AS missing_map_age,
    dictGetOrDefault(
        'mixed_test_range_hashed',
        'profile',
        999,
        dt,
        '{"city":"Unknown","age":0}'::JSON
    ).city AS missing_json_city,
    dictGetOrDefault(
        'mixed_test_range_hashed',
        'profile',
        999,
        dt,
        '{"city":"Unknown","age":0}'::JSON
    ).age AS missing_json_age
FROM
(
    SELECT 1 AS id, toDate('2024-01-15') AS dt
    UNION ALL SELECT 2, toDate('2024-02-15')
    UNION ALL SELECT 3, toDate('2024-03-15')
)
ORDER BY id;

-- Section E. Verify mixed dictionaries
SELECT '=== Mixed Dictionary Info ===' as test;
SELECT name, type, key, attribute.names, attribute.types
FROM system.dictionaries
WHERE name LIKE 'mixed_test%' AND database = currentDatabase()
ORDER BY name;

-- Section F. `dictGet` missing key materialization (no explicit default)
SELECT '=== dictGet missing key materialization (Map + JSON profile) - FLAT ===' as test;
SELECT
    dictGet('mixed_test_flat', 'metadata', 999)['city'] AS missing_map_city,
    dictGet('mixed_test_flat', 'metadata', 999)['age'] AS missing_map_age,
    dictGet('mixed_test_flat', 'profile', 999).city AS missing_json_city,
    dictGet('mixed_test_flat', 'profile', 999).age AS missing_json_age;

SELECT '=== dictGet missing key materialization (Map + JSON profile) - HASHED ===' as test;
SELECT
    dictGet('mixed_test_hashed', 'metadata', 999)['city'] AS missing_map_city,
    dictGet('mixed_test_hashed', 'metadata', 999)['age'] AS missing_map_age,
    dictGet('mixed_test_hashed', 'profile', 999).city AS missing_json_city,
    dictGet('mixed_test_hashed', 'profile', 999).age AS missing_json_age;

SELECT '=== dictGet missing key materialization (Map + JSON profile) - HASHED_ARRAY ===' as test;
SELECT
    dictGet('mixed_test_hashed_array', 'metadata', 999)['city'] AS missing_map_city,
    dictGet('mixed_test_hashed_array', 'metadata', 999)['age'] AS missing_map_age,
    dictGet('mixed_test_hashed_array', 'profile', 999).city AS missing_json_city,
    dictGet('mixed_test_hashed_array', 'profile', 999).age AS missing_json_age;

SELECT '=== dictGet missing key materialization (Map + JSON profile) - CACHE ===' as test;
SELECT
    dictGet('mixed_test_cache', 'metadata', 999)['city'] AS missing_map_city,
    dictGet('mixed_test_cache', 'metadata', 999)['age'] AS missing_map_age,
    dictGet('mixed_test_cache', 'profile', 999).city AS missing_json_city,
    dictGet('mixed_test_cache', 'profile', 999).age AS missing_json_age;

SELECT '=== dictGet missing key materialization (Map + JSON profile) - RANGE_HASHED ===' as test;
SELECT
    dictGet('mixed_test_range_hashed', 'metadata', 999, dt)['city'] AS missing_map_city,
    dictGet('mixed_test_range_hashed', 'metadata', 999, dt)['age'] AS missing_map_age,
    dictGet('mixed_test_range_hashed', 'profile', 999, dt).city AS missing_json_city,
    dictGet('mixed_test_range_hashed', 'profile', 999, dt).age AS missing_json_age
FROM (SELECT toDate('2024-01-15') AS dt);

-- Section G. Typed JSON paths via `CAST` (FLAT only; layout-independent behavior)
-- Validates `CAST(dictGet(...) AS JSON(city String, age UInt32))` + `toTypeName`.

SELECT '=== Typed JSON paths - dictGetOrDefault missing key (cast) - FLAT ===' as test;
SELECT
    CAST(dictGetOrDefault('mixed_test_flat', 'profile', 999, '{"city":"Unknown","age":0}'::JSON) AS JSON(city String, age UInt32)).age AS missing_age,
    toTypeName(CAST(dictGetOrDefault('mixed_test_flat', 'profile', 999, '{"city":"Unknown","age":0}'::JSON) AS JSON(city String, age UInt32)).age) AS missing_age_type,
    CAST(dictGetOrDefault('mixed_test_flat', 'profile', 999, '{"city":"Unknown","age":0}'::JSON) AS JSON(city String, age UInt32)).city AS missing_city;

SELECT '=== Typed JSON paths - dictGet missing key (cast) - FLAT ===' as test;
SELECT
    CAST(dictGet('mixed_test_flat', 'profile', 999) AS JSON(city String, age UInt32)).age AS missing_age,
    toTypeName(CAST(dictGet('mixed_test_flat', 'profile', 999) AS JSON(city String, age UInt32)).age) AS missing_age_type,
    CAST(dictGet('mixed_test_flat', 'profile', 999) AS JSON(city String, age UInt32)).city AS missing_city;

-- Section H. JSON nested object and array access (per layout)
-- Covers:
-- - nested path (`.skills.programming`)
-- - array and array element (`.hobbies`, `.hobbies[1]`)
-- - deep nested scalar (`.preferences.display.font_size`)
-- - boolean path (`.preferences.privacy.share_data`)
-- FLAT also validates typed JSON readback with `CAST` + `toTypeName`.

SELECT '=== JSON nested/array/sub-object access - FLAT ===' as test;
SELECT
    id,
    dictGet('mixed_test_flat', 'profile', id).hobbies AS hobbies,
    dictGet('mixed_test_flat', 'profile', id).hobbies[1] AS first_hobby,
    dictGet('mixed_test_flat', 'profile', id).skills.programming AS prog_skills,
    dictGet('mixed_test_flat', 'preferences', id).display.font_size AS font_size,
    dictGet('mixed_test_flat', 'preferences', id).privacy.share_data AS share_data,
    CAST(dictGet('mixed_test_flat', 'profile', id) AS JSON(city String, age UInt32)).age AS typed_age,
    toTypeName(CAST(dictGet('mixed_test_flat', 'profile', id) AS JSON(city String, age UInt32)).age) AS typed_age_type
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3)
ORDER BY id;

SELECT '=== JSON nested/array/sub-object access - HASHED ===' as test;
SELECT
    id,
    dictGet('mixed_test_hashed', 'profile', id).hobbies AS hobbies,
    dictGet('mixed_test_hashed', 'profile', id).hobbies[1] AS first_hobby,
    dictGet('mixed_test_hashed', 'profile', id).skills.programming AS prog_skills,
    dictGet('mixed_test_hashed', 'preferences', id).display.font_size AS font_size,
    dictGet('mixed_test_hashed', 'preferences', id).privacy.share_data AS share_data
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3)
ORDER BY id;

SELECT '=== JSON nested/array/sub-object access - HASHED_ARRAY ===' as test;
SELECT
    id,
    dictGet('mixed_test_hashed_array', 'profile', id).hobbies AS hobbies,
    dictGet('mixed_test_hashed_array', 'profile', id).hobbies[1] AS first_hobby,
    dictGet('mixed_test_hashed_array', 'profile', id).skills.programming AS prog_skills,
    dictGet('mixed_test_hashed_array', 'preferences', id).display.font_size AS font_size,
    dictGet('mixed_test_hashed_array', 'preferences', id).privacy.share_data AS share_data
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3)
ORDER BY id;

SELECT '=== JSON nested/array/sub-object access - CACHE ===' as test;
SELECT
    id,
    dictGet('mixed_test_cache', 'profile', id).hobbies AS hobbies,
    dictGet('mixed_test_cache', 'profile', id).hobbies[1] AS first_hobby,
    dictGet('mixed_test_cache', 'profile', id).skills.programming AS prog_skills,
    dictGet('mixed_test_cache', 'preferences', id).display.font_size AS font_size,
    dictGet('mixed_test_cache', 'preferences', id).privacy.share_data AS share_data
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3)
ORDER BY id;

SELECT '=== JSON nested/array/sub-object access - RANGE_HASHED ===' as test;
SELECT
    id,
    dictGet('mixed_test_range_hashed', 'profile', id, dt).hobbies AS hobbies,
    dictGet('mixed_test_range_hashed', 'profile', id, dt).hobbies[1] AS first_hobby,
    dictGet('mixed_test_range_hashed', 'profile', id, dt).skills.programming AS prog_skills,
    dictGet('mixed_test_range_hashed', 'preferences', id, dt).display.font_size AS font_size,
    dictGet('mixed_test_range_hashed', 'preferences', id, dt).privacy.share_data AS share_data
FROM
(
    SELECT 1 AS id, toDate('2024-01-15') AS dt
    UNION ALL SELECT 2, toDate('2024-02-15')
    UNION ALL SELECT 3, toDate('2024-03-15')
)
ORDER BY id;

-- Section I. JSON type introspection (`dynamicType`, `toTypeName`)
SELECT '=== JSON type introspection (dynamicType + toTypeName) ===' as test;
SELECT
    dynamicType(dictGet('mixed_test_flat', 'profile', 1).age) AS age_dynamic_type,
    dynamicType(dictGet('mixed_test_flat', 'profile', 1).city) AS city_dynamic_type,
    dynamicType(dictGet('mixed_test_flat', 'profile', 1).hobbies) AS hobbies_dynamic_type,
    dynamicType(dictGet('mixed_test_flat', 'settings', 1).notifications) AS bool_dynamic_type,
    toTypeName(dictGet('mixed_test_flat', 'profile', 1)) AS profile_type_name,
    toTypeName(dictGet('mixed_test_flat', 'settings', 1)) AS settings_type_name;

-- Section J. JSON path introspection (`JSONAllPaths*`, `JSONDynamicPaths`)
SELECT '=== JSONAllPaths on dictGet JSON result ===' as test;
SELECT
    JSONAllPaths(dictGet('mixed_test_flat', 'profile', 1)) AS profile_all_paths;

SELECT '=== JSONAllPathsWithTypes on dictGet JSON result ===' as test;
SELECT
    JSONAllPathsWithTypes(dictGet('mixed_test_flat', 'profile', 1)) AS profile_paths_with_types;

SELECT '=== JSONDynamicPaths on dictGet JSON result ===' as test;
SELECT
    JSONDynamicPaths(dictGet('mixed_test_flat', 'profile', 1)) AS profile_dynamic_paths;

-- Section K. JSON value comparison via `dictGet`
SELECT '=== JSON value comparison ===' as test;
SELECT
    dictGet('mixed_test_flat', 'profile', 1) = dictGet('mixed_test_flat', 'profile', 1) AS self_equal,
    dictGet('mixed_test_flat', 'profile', 1) = dictGet('mixed_test_flat', 'profile', 2) AS diff_profiles_equal,
    dictGet('mixed_test_flat', 'settings', 1) = dictGet('mixed_test_flat', 'settings', 3) AS same_theme_settings_equal,
    dictGet('mixed_test_flat', 'profile', 1) < dictGet('mixed_test_flat', 'profile', 2) AS profile_less,
    dictGet('mixed_test_flat', 'profile', 1) > dictGet('mixed_test_flat', 'profile', 2) AS profile_greater;

-- Section M. JSON `::Type` cast syntax on `dictGet` results
SELECT '=== JSON cast syntax (::String, ::Int64) on sub-paths ===' as test;
SELECT
    dictGet('mixed_test_flat', 'profile', 1).age::UInt64 AS age_as_uint64,
    dictGet('mixed_test_flat', 'profile', 1).city::String AS city_as_string,
    dictGet('mixed_test_flat', 'preferences', 1).display.font_size::UInt32 AS font_size_as_uint32,
    toTypeName(dictGet('mixed_test_flat', 'profile', 1).age::UInt64) AS casted_age_type,
    toTypeName(dictGet('mixed_test_flat', 'profile', 1).city::String) AS casted_city_type;

-- ============================================
-- Section O. `JSON(path TypeName, SKIP)` in dictionary definitions
-- ============================================

CREATE TABLE IF NOT EXISTS typed_json_dict_test_source
(
    id UInt64,
    payload JSON
)
ENGINE = Memory;

INSERT INTO typed_json_dict_test_source VALUES
(1, '{"name":"Alice","age":30,"city":"New York","debug_x":1}'),
(2, '{"name":"Bob","age":25,"city":"London","debug_x":2}'),
(3, '{"name":"Charlie","age":35,"city":"Tokyo","debug_x":3}');

CREATE DICTIONARY IF NOT EXISTS typed_json_dict_test_flat
(
    id UInt64,
    payload JSON(
        name String,
        age UInt32,
        SKIP REGEXP 'debug_.*'
    )
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB currentDatabase()
    TABLE 'typed_json_dict_test_source'
))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 0);

SELECT '=== JSON typed path + SKIP in dictionary definition ===' as test;
SELECT
    id,
    dictGet('typed_json_dict_test_flat', 'payload', id).name AS name,
    dictGet('typed_json_dict_test_flat', 'payload', id).age AS age,
    dictGet('typed_json_dict_test_flat', 'payload', id).city AS city,
    toTypeName(dictGet('typed_json_dict_test_flat', 'payload', id).name) AS name_type,
    toTypeName(dictGet('typed_json_dict_test_flat', 'payload', id).age) AS age_type,
    toTypeName(dictGet('typed_json_dict_test_flat', 'payload', id).city) AS city_type
FROM (SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3)
ORDER BY id;

SELECT '=== JSON typed path + SKIP missing key materialization ===' as test;
SELECT
    dictGet('typed_json_dict_test_flat', 'payload', 999).name AS missing_name,
    dictGet('typed_json_dict_test_flat', 'payload', 999).age AS missing_age,
    dictGet('typed_json_dict_test_flat', 'payload', 1).debug_x AS skipped_debug_x,
    toTypeName(dictGet('typed_json_dict_test_flat', 'payload', 999).name) AS missing_name_type,
    toTypeName(dictGet('typed_json_dict_test_flat', 'payload', 999).age) AS missing_age_type;

-- ============================================
-- Section O2. Negative tests: `Nullable(Map(...))` / `Nullable(Array(...))`
-- These types are expected to be rejected with `Code: 43`.
-- ============================================

CREATE TABLE IF NOT EXISTS nullable_nested_source
(
    id UInt64,
    m Map(String, String),
    a Array(String)
)
ENGINE = Memory;

INSERT INTO nullable_nested_source VALUES (1, {'k': 'v'}, ['x', 'y']);

CREATE DICTIONARY IF NOT EXISTS nullable_map_dict_test
(
    id UInt64,
    m Nullable(Map(String, String))
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB currentDatabase()
    TABLE 'nullable_nested_source'
))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 0); -- { serverError 43 }

CREATE DICTIONARY IF NOT EXISTS nullable_array_dict_test
(
    id UInt64,
    a Nullable(Array(String))
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB currentDatabase()
    TABLE 'nullable_nested_source'
))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 0); -- { serverError 43 }

-- ============================================
-- Section P. Deep nested JSON dictionary tests
-- ============================================

-- P1. Create source table with deep nested JSON
CREATE TABLE IF NOT EXISTS deep_json_test_source
(
    id UInt64,
    name String,
    deep_data JSON,
    nested_structure JSON,
    complex_hierarchy JSON
)
ENGINE = Memory;

-- P2. Insert deep nested JSON test data
INSERT INTO deep_json_test_source VALUES
(1, 'DeepTest1', '{"level1":{"level2":{"level3":{"level4":{"level5":{"value":"deep_value_1","numbers":[1,2,3],"nested":{"key":"nested_value_1"}}}}}}}', '{"company":{"departments":[{"name":"Engineering","teams":[{"name":"Backend","members":[{"name":"Alice","role":"Senior","projects":[{"name":"Project A","status":"active","details":{"start_date":"2024-01-01","budget":100000}}]}]}]},{"name":"Product","teams":[{"name":"Design","members":[{"name":"Bob","role":"Lead","projects":[{"name":"Project B","status":"planning"}]}]}]}]}}', '{"user":{"profile":{"personal":{"name":"Charlie","age":30,"address":{"street":"123 Main St","city":"New York","country":{"code":"US","name":"United States","regions":[{"name":"Northeast","states":[{"name":"NY","cities":[{"name":"NYC","boroughs":["Manhattan","Brooklyn"]}]}]}]}}},"work":{"company":"Tech Corp","position":"Engineer","projects":[{"id":1,"name":"Alpha","team":{"lead":"David","members":["Eve","Frank"]}}]}}}}');

INSERT INTO deep_json_test_source VALUES
(2, 'DeepTest2', '{"level1":{"level2":{"level3":{"level4":{"level5":{"level6":{"value":"very_deep_value","array":[{"item":"a","sub":{"data":"x"}},{"item":"b","sub":{"data":"y"}}]}}}}}}}', '{"organization":{"divisions":{"tech":{"teams":{"frontend":{"members":[{"id":1,"name":"Alice","skills":{"languages":["JS","TS"],"frameworks":{"react":{"version":"18","projects":[{"name":"App1","features":["feature1","feature2"]}]}}}}]},"backend":{"members":[{"id":2,"name":"Bob","skills":{"languages":["Python","Go"],"databases":{"primary":"PostgreSQL","cache":"Redis"}}}]}}}}}}', '{"system":{"config":{"database":{"connections":{"primary":{"host":"db1.example.com","port":5432,"replicas":[{"host":"db1-replica1.example.com","port":5432,"region":"us-east-1"},{"host":"db1-replica2.example.com","port":5432,"region":"us-west-2"}]},"secondary":{"host":"db2.example.com","port":5432}}},"cache":{"redis":{"cluster":{"nodes":[{"host":"redis1","port":6379,"slots":[0,5460]},{"host":"redis2","port":6379,"slots":[5461,10922]}]}}}}}}');

INSERT INTO deep_json_test_source VALUES
(3, 'DeepTest3', '{"a":{"b":{"c":{"d":{"e":{"f":{"value":"extremely_deep","metadata":{"created":"2024-01-01","tags":["tag1","tag2"],"nested_obj":{"key1":"val1","key2":"val2"}}}}}}}}}', '{"root":{"branch1":{"leaf1":{"data":"value1","children":[{"id":1,"name":"child1","attributes":{"color":"red","size":"large","details":{"material":"wood","origin":{"country":"USA","state":"CA"}}}},{"id":2,"name":"child2","attributes":{"color":"blue","size":"medium"}}]},"leaf2":{"data":"value2","children":[{"id":3,"name":"child3"}]}},"branch2":{"leaf3":{"data":"value3"}}}}', '{"application":{"modules":{"auth":{"providers":[{"type":"oauth2","config":{"client_id":"abc123","endpoints":{"authorization":"https://auth.example.com/authorize","token":"https://auth.example.com/token","userinfo":{"url":"https://auth.example.com/userinfo","headers":{"Authorization":"Bearer token"}}}}},{"type":"saml","config":{"entity_id":"saml-entity","sso_url":"https://saml.example.com/sso"}}]},"api":{"version":"v1","endpoints":[{"path":"/users","methods":["GET","POST"],"middleware":["auth","logging"],"handlers":{"GET":{"function":"getUsers","params":{"limit":100,"offset":0}}}}]}}}}');

-- P3. Create `FLAT` dictionary for deep nested JSON
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
    DB currentDatabase()
    TABLE 'deep_json_test_source'
))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 0);

-- P4. Create `HASHED` dictionary for deep nested JSON
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
    DB currentDatabase()
    TABLE 'deep_json_test_source'
))
LAYOUT(HASHED())
LIFETIME(MIN 0 MAX 0);

-- P5. Test deep nested JSON access
SELECT '=== Testing Deep Nested JSON ===' as test;
SELECT
    id,
    dictGet('deep_json_test_flat', 'name', id) as name,
    dictGet('deep_json_test_flat', 'deep_data', id).level1.level2.level3.level4.level5.value as deep_value_5,
    dictGet('deep_json_test_flat', 'nested_structure', id).company.departments[1].teams[1].members[1].name as member_name,
    dictGet('deep_json_test_flat', 'complex_hierarchy', id).user.profile.personal.address.country.name as country_name
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3)
ORDER BY id;

-- P6. Test `HASHED` dictionary with deep nested JSON
SELECT '=== Testing HASHED Dictionary with Deep Nested JSON ===' as test;
SELECT
    id,
    dictGet('deep_json_test_hashed', 'name', id) as name,
    dictGet('deep_json_test_hashed', 'deep_data', id).level1.level2.level3.level4.level5.value as deep_value,
    dictGet('deep_json_test_hashed', 'nested_structure', id).company.departments[1].teams[1].members[1].name as member_name
FROM (SELECT 1 as id UNION ALL SELECT 2 UNION ALL SELECT 3)
ORDER BY id;

-- P7. Verify deep JSON dictionary info
SELECT '=== Deep JSON Dictionary Info ===' as test;
SELECT name, type, key, attribute.names, attribute.types
FROM system.dictionaries
WHERE name LIKE 'deep_json_test%' AND database = currentDatabase()
ORDER BY name;

-- ============================================
-- Section Q. `Nullable(JSON)` dictionary tests
-- ============================================

-- Q1. Create source table with `Nullable(JSON)`
CREATE TABLE IF NOT EXISTS nullable_json_test_source
(
    id UInt64,
    start_date Date,
    end_date Date,
    name String,
    profile Nullable(JSON),
    email Nullable(String)
)
ENGINE = MergeTree
ORDER BY id;

-- Q2. Insert test data with some NULL values
INSERT INTO nullable_json_test_source (id, start_date, end_date, name, profile, email) VALUES
(1, toDate('2024-01-01'), toDate('2024-01-31'), 'Alice', '{"age": 30, "city": "New York"}', 'alice@example.com'),
(2, toDate('2024-02-01'), toDate('2024-02-29'), 'Bob', NULL, NULL),
(3, toDate('2024-03-01'), toDate('2024-03-31'), 'Charlie', '{"age": 35, "city": "Tokyo"}', 'charlie@example.com'),
(4, toDate('2024-04-01'), toDate('2024-04-30'), 'David', NULL, NULL);

-- Q3. Create `FLAT` dictionary with `Nullable(JSON)`
CREATE DICTIONARY IF NOT EXISTS nullable_json_test_flat
(
    id UInt64,
    name String,
    profile Nullable(JSON),
    email Nullable(String)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB currentDatabase()
    TABLE 'nullable_json_test_source'
))
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 0);

-- Q4. Create `HASHED_ARRAY` dictionary with `Nullable(JSON)`
CREATE DICTIONARY IF NOT EXISTS nullable_json_test_hashed_array
(
    id UInt64,
    name String,
    profile Nullable(JSON),
    email Nullable(String)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB currentDatabase()
    TABLE 'nullable_json_test_source'
))
LAYOUT(HASHED_ARRAY())
LIFETIME(MIN 0 MAX 0);

-- Q5. Create `HASHED` dictionary with `Nullable(JSON)`
CREATE DICTIONARY IF NOT EXISTS nullable_json_test_hashed
(
    id UInt64,
    name String,
    profile Nullable(JSON),
    email Nullable(String)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB currentDatabase()
    TABLE 'nullable_json_test_source'
))
LAYOUT(HASHED())
LIFETIME(MIN 0 MAX 0);

-- Q6. Create `CACHE` dictionary with `Nullable(JSON)`
CREATE DICTIONARY IF NOT EXISTS nullable_json_test_cache
(
    id UInt64,
    name String,
    profile Nullable(JSON),
    email Nullable(String)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB currentDatabase()
    TABLE 'nullable_json_test_source'
))
LAYOUT(CACHE(SIZE_IN_CELLS 1024))
LIFETIME(MIN 0 MAX 0);

-- Q7. Create `RANGE_HASHED` dictionary with `Nullable(JSON)`
CREATE DICTIONARY IF NOT EXISTS nullable_json_test_range_hashed
(
    id UInt64,
    start_date Date,
    end_date Date,
    name String,
    profile Nullable(JSON),
    email Nullable(String)
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB currentDatabase()
    TABLE 'nullable_json_test_source'
))
LAYOUT(RANGE_HASHED())
LIFETIME(MIN 0 MAX 0)
RANGE(MIN start_date MAX end_date);

-- Q8. Test NULL value detection (`FLAT`)
SELECT '=== Testing NULL Value Detection - FLAT ===' as test;
SELECT
    number AS id,
    dictGet('nullable_json_test_flat', 'name', number) AS name,
    isNull(dictGet('nullable_json_test_flat', 'profile', number)) AS profile_is_null,
    isNull(dictGet('nullable_json_test_flat', 'email', number)) AS email_is_null
FROM numbers(1, 4)
ORDER BY id;

-- Q9. Test NULL value detection (`HASHED_ARRAY`) including missing key materialization
SELECT '=== Testing NULL Value Detection - HASHED_ARRAY ===' as test;
SELECT
    number AS id,
    isNull(dictGet('nullable_json_test_hashed_array', 'profile', number)) AS profile_is_null,
    toTypeName(dictGet('nullable_json_test_hashed_array', 'profile', number)) AS profile_type
FROM numbers(1, 5)
ORDER BY id;

-- Q10. Test NULL value detection (`HASHED`) including missing key materialization
SELECT '=== Testing NULL Value Detection - HASHED ===' as test;
SELECT
    number AS id,
    isNull(dictGet('nullable_json_test_hashed', 'profile', number)) AS profile_is_null,
    toTypeName(dictGet('nullable_json_test_hashed', 'profile', number)) AS profile_type
FROM numbers(1, 5)
ORDER BY id;

-- Q11. Test NULL value detection (`CACHE`) including missing key materialization
SELECT '=== Testing NULL Value Detection - CACHE ===' as test;
SELECT
    number AS id,
    isNull(dictGet('nullable_json_test_cache', 'profile', number)) AS profile_is_null,
    toTypeName(dictGet('nullable_json_test_cache', 'profile', number)) AS profile_type
FROM numbers(1, 5)
ORDER BY id;

-- Q12. Test NULL value detection (`RANGE_HASHED`) including missing key materialization
SELECT '=== Testing NULL Value Detection - RANGE_HASHED ===' as test;
SELECT
    id,
    isNull(value) AS profile_is_null,
    toTypeName(value) AS profile_type
FROM
(
    SELECT
        id,
        dictGet('nullable_json_test_range_hashed', 'profile', id, dt) AS value
    FROM
    (
        SELECT 1 AS id, toDate('2024-01-15') AS dt
        UNION ALL SELECT 2, toDate('2024-02-15')
        UNION ALL SELECT 3, toDate('2024-03-15')
        UNION ALL SELECT 999, toDate('2024-03-15')
    )
)
ORDER BY id;

-- Q13. Test NULL value detection reading `RANGE_HASHED` dictionary as a table
-- This exercises `getColumnInternal` (the dictionary-as-table path) rather than `dictGet`.
SELECT '=== Testing NULL Value Detection - RANGE_HASHED (table read) ===' as test;
SELECT
    id,
    isNull(profile) AS profile_is_null,
    isNull(email) AS email_is_null
FROM nullable_json_test_range_hashed
ORDER BY id;

-- ============================================
-- Section R. `IP_TRIE` layout tests
-- ============================================

-- R1. Create dedicated source table for `IP_TRIE` tests
CREATE TABLE IF NOT EXISTS ip_trie_test_source
(
    prefix String,
    name String,
    ip_meta Map(String, String),
    ip_stats Map(String, Float64),
    ip_profile JSON
)
ENGINE = MergeTree
ORDER BY prefix;

-- R2. Insert source data for IPv4/IPv6 longest-prefix matching
INSERT INTO ip_trie_test_source VALUES
(
    '10.0.0.0/8',
    'Corp-Private-Backbone',
    {'asn': 'AS64512', 'country': 'US', 'network_type': 'enterprise-private', 'ip_family': 'ipv4'},
    {'prefix_len': 8.0, 'risk_score': 0.15},
    '{"asn": 64512, "registry": "Private ASN", "category": "enterprise-private", "country": "US"}'
),
(
    '10.10.0.0/16',
    'Corp-Private-DC1',
    {'asn': 'AS64512', 'country': 'US', 'network_type': 'datacenter-subnet', 'ip_family': 'ipv4'},
    {'prefix_len': 16.0, 'risk_score': 0.05},
    '{"asn": 64512, "registry": "Private ASN", "category": "datacenter-subnet", "country": "US"}'
),
(
    '2001:db8::/32',
    'ExampleV6-Doc-Global',
    {'asn': 'AS64496', 'country': 'DOC', 'network_type': 'documentation', 'ip_family': 'ipv6'},
    {'prefix_len': 32.0, 'risk_score': 0.5},
    '{"asn": 64496, "registry": "IETF Documentation", "category": "documentation", "country": "DOC"}'
),
(
    '2001:db8:1::/48',
    'ExampleV6-Doc-RegionA',
    {'asn': 'AS64496', 'country': 'DOC', 'network_type': 'documentation-subnet', 'ip_family': 'ipv6'},
    {'prefix_len': 48.0, 'risk_score': 0.2},
    '{"asn": 64496, "registry": "IETF Documentation", "category": "documentation-subnet", "country": "DOC"}'
);

-- R3. Create one `IP_TRIE` dictionary with both `Map` and `JSON` attributes
CREATE DICTIONARY IF NOT EXISTS ip_trie_test_combined
(
    prefix String,
    name String,
    ip_meta Map(String, String),
    ip_stats Map(String, Float64),
    ip_profile JSON
)
PRIMARY KEY prefix
SOURCE(CLICKHOUSE(
    HOST '127.0.0.1'
    PORT tcpPort()
    USER 'default'
    PASSWORD ''
    DB currentDatabase()
    TABLE 'ip_trie_test_source'
))
LAYOUT(IP_TRIE())
LIFETIME(MIN 0 MAX 0);

-- R4. Test `Map` and `JSON` in one statement
SELECT '=== IP_TRIE one-statement Map+JSON test ===' as test;
SELECT
    dictGet('ip_trie_test_combined', 'name', toIPv4('10.10.1.1')) AS name_v4,
    dictGet('ip_trie_test_combined', 'ip_meta', toIPv4('10.10.1.1'))['network_type'] AS map_network_type_v4,
    dictGet('ip_trie_test_combined', 'ip_stats', IPv6StringToNum('2001:db8:1::1'))['prefix_len'] AS map_prefix_len_v6,
    dictGet('ip_trie_test_combined', 'ip_profile', toIPv4('10.10.1.1')).asn AS json_asn_v4,
    dictGet('ip_trie_test_combined', 'ip_profile', IPv6StringToNum('2001:db8:1::1')).category AS json_category_v6;

-- Cleanup: Drop dictionaries and tables
DROP DICTIONARY IF EXISTS mixed_test_flat;
DROP DICTIONARY IF EXISTS mixed_test_hashed;
DROP DICTIONARY IF EXISTS mixed_test_hashed_array;
DROP DICTIONARY IF EXISTS mixed_test_cache;
DROP DICTIONARY IF EXISTS mixed_test_range_hashed;
DROP DICTIONARY IF EXISTS typed_json_dict_test_flat;
DROP DICTIONARY IF EXISTS nullable_map_dict_test;
DROP DICTIONARY IF EXISTS nullable_array_dict_test;
DROP DICTIONARY IF EXISTS ip_trie_test_combined;
DROP DICTIONARY IF EXISTS deep_json_test_flat;
DROP DICTIONARY IF EXISTS deep_json_test_hashed;
DROP DICTIONARY IF EXISTS nullable_json_test_flat;
DROP DICTIONARY IF EXISTS nullable_json_test_hashed_array;
DROP DICTIONARY IF EXISTS nullable_json_test_hashed;
DROP DICTIONARY IF EXISTS nullable_json_test_cache;
DROP DICTIONARY IF EXISTS nullable_json_test_range_hashed;

DROP TABLE IF EXISTS mixed_test_source;
DROP TABLE IF EXISTS typed_json_dict_test_source;
DROP TABLE IF EXISTS nullable_nested_source;
DROP TABLE IF EXISTS deep_json_test_source;
DROP TABLE IF EXISTS nullable_json_test_source;
DROP TABLE IF EXISTS ip_trie_test_source;
