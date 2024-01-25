DROP TABLE IF EXISTS 02919_test_table_noarg;
CREATE TABLE 02919_test_table_noarg(str String) ENGINE = FuzzJSON('{}');

SELECT count() FROM (SELECT * FROM 02919_test_table_noarg LIMIT 100);

DROP TABLE IF EXISTS 02919_test_table_noarg;

--
DROP TABLE IF EXISTS 02919_test_table_valid_args;
CREATE TABLE 02919_test_table_valid_args(str String) ENGINE = FuzzJSON(
    '{"pet":"rat"}', NULL);

SELECT count() FROM (SELECT * FROM 02919_test_table_valid_args LIMIT 100);

DROP TABLE IF EXISTS 02919_test_table_valid_args;

--
DROP TABLE IF EXISTS 02919_test_table_reuse_args;
CREATE TABLE 02919_test_table_reuse_args(str String) ENGINE = FuzzJSON(
    '{
      "name": "Jane Doe",
      "age": 30,
      "city": "New York",
      "contacts": {
        "email": "jane@example.com",
        "phone": "+1234567890"
      },
      "skills": [
        "JavaScript",
        "Python",
        {
          "frameworks": ["React", "Django"]
        }
      ],
      "projects": [
        {"name": "Project A", "status": "completed"},
        {"name": "Project B", "status": "in-progress"}
      ]
    }',
    12345);

SELECT count() FROM (SELECT * FROM 02919_test_table_reuse_args LIMIT 100);

DROP TABLE IF EXISTS 02919_test_table_reuse_args;
