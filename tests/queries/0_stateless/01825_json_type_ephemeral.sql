
SET enable_json_type = 1;

DROP TABLE IF EXISTS t_github_json;

CREATE table t_github_json
(
    event_type LowCardinality(String) DEFAULT JSONExtractString(message_raw, 'type'),
    repo_name LowCardinality(String) DEFAULT JSONExtractString(message_raw, 'repo', 'name'),
    message JSON DEFAULT empty(message_raw) ? '{}' : message_raw,
    message_raw String EPHEMERAL
) ENGINE = MergeTree ORDER BY (event_type, repo_name);

INSERT INTO t_github_json (message_raw) FORMAT JSONEachRow {"message_raw": "{\"type\":\"PushEvent\", \"created_at\": \"2022-01-04 07:00:00\", \"actor\":{\"avatar_url\":\"https://avatars.githubusercontent.com/u/123213213?\",\"display_login\":\"github-actions\",\"gravatar_id\":\"\",\"id\":123123123,\"login\":\"github-actions[bot]\",\"url\":\"https://api.github.com/users/github-actions[bot]\"},\"repo\":{\"id\":1001001010101,\"name\":\"some-repo\",\"url\":\"https://api.github.com/repos/some-repo\"}}"}

SELECT * FROM t_github_json ORDER BY event_type, repo_name;

DROP TABLE t_github_json;
