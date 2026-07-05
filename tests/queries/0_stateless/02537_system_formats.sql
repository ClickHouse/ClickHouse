SELECT * EXCEPT (description, examples, introduced_in, related) FROM system.formats WHERE name IN ('CSV', 'Native') ORDER BY name;
