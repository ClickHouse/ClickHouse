-- Tags: stateful
SELECT table, name, type, default_kind, default_expression FROM system.columns WHERE database = 'test' AND table = 'hits'
