SELECT '-- The clauses are optional and can be written in any order; the formatted query is canonical';
SELECT formatQuerySingleLine('CREATE TOKEN');
SELECT formatQuerySingleLine($$CREATE TOKEN VALID UNTIL '2077-01-01'$$);
SELECT formatQuerySingleLine('CREATE TOKEN VALID FOR INTERVAL 30 DAY');
SELECT formatQuerySingleLine('CREATE TOKEN GRANTS (SELECT ON db.t)');
SELECT formatQuerySingleLine($$CREATE TOKEN VALID UNTIL '2077-01-01' GRANTS (SELECT ON db.t, INSERT ON db.t)$$);
SELECT formatQuerySingleLine($$CREATE TOKEN GRANTS (SELECT ON db.t) VALID UNTIL '2077-01-01'$$);
SELECT formatQuerySingleLine('CREATE TOKEN VALID FOR INTERVAL 1 DAY GRANTS (USAGE ON *.*) FORMAT TSVRaw');
SELECT formatQuerySingleLine('CREATE TOKEN SETTINGS create_token_default_ttl_seconds = 0 FORMAT TSVRaw');

SELECT '-- A grant of an authentication method is never widened by the backward-compatibility rewrites';
SELECT formatQuerySingleLine('CREATE TOKEN GRANTS (ALTER USER ON alice)');

SELECT '-- Malformed clauses are rejected';
SELECT formatQuerySingleLine('CREATE TOKEN GRANTS ()'); -- { serverError SYNTAX_ERROR }
SELECT formatQuerySingleLine('CREATE TOKEN GRANTS SELECT ON db.t'); -- { serverError SYNTAX_ERROR }
SELECT formatQuerySingleLine('CREATE TOKEN VALID UNTIL'); -- { serverError SYNTAX_ERROR }
SELECT formatQuerySingleLine('CREATE TOKEN GRANTS (SELECT ON db.t) GRANTS (SELECT ON db.t2)'); -- { serverError SYNTAX_ERROR }
SELECT formatQuerySingleLine($$CREATE TOKEN VALID UNTIL '2077-01-01' VALID UNTIL '2078-01-01'$$); -- { serverError SYNTAX_ERROR }
SELECT formatQuerySingleLine('CREATE TOKEN FOR alice'); -- { serverError SYNTAX_ERROR }
