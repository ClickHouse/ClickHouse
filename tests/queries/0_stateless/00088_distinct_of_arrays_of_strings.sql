-- Random settings limits: max_bytes_before_external_distinct=(0, 0); max_bytes_ratio_before_external_distinct=(0, 0)
-- Without ORDER BY the rows come in the order of DISTINCT, which a spill to disk changes.
SELECT DISTINCT arrayFilter(x -> notEmpty(x), arrayJoin([[''], ['is_registred'], ['registration_month','user_login','is_registred'], ['is_registred'], ['is_registred'], ['']]));
