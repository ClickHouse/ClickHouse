-- An `Alias` exposes the metadata of the final table of its chain, so a parameterized view reached
-- through `Alias` tables must be treated as a parameterized view by `DESCRIBE` and by `Merge`,
-- however long the chain is.

SET use_declared_schema_for_parameterized_views = 1;

CREATE VIEW 05315_pv_no_schema AS SELECT number AS n FROM numbers({upper_bound:UInt64});
CREATE VIEW 05315_pv_declared (n UInt64 COMMENT 'declared') AS SELECT number AS n FROM numbers({upper_bound:UInt64});

CREATE TABLE 05315_alias_no_schema ENGINE = Alias(currentDatabase(), '05315_pv_no_schema');
CREATE TABLE 05315_alias_declared ENGINE = Alias(currentDatabase(), '05315_pv_declared');

-- { echoOn }

-- A schemaless parameterized view cannot be described without parameters, also through an `Alias`.
DESCRIBE TABLE 05315_pv_no_schema; -- { serverError UNSUPPORTED_METHOD }
DESCRIBE TABLE 05315_alias_no_schema; -- { serverError UNSUPPORTED_METHOD }

-- A declared schema is described directly and through an `Alias`.
DESCRIBE TABLE 05315_pv_declared;
DESCRIBE TABLE 05315_alias_declared;

-- { echoOff }

DROP TABLE 05315_alias_declared;
DROP TABLE 05315_alias_no_schema;

-- A chain of 70 `Alias` tables. An `Alias` refuses an existing `Alias` as its target, so the chain is
-- built from the outer end.
CREATE TABLE 05315_a1 ENGINE = Alias(currentDatabase(), '05315_a2');
CREATE TABLE 05315_a2 ENGINE = Alias(currentDatabase(), '05315_a3');
CREATE TABLE 05315_a3 ENGINE = Alias(currentDatabase(), '05315_a4');
CREATE TABLE 05315_a4 ENGINE = Alias(currentDatabase(), '05315_a5');
CREATE TABLE 05315_a5 ENGINE = Alias(currentDatabase(), '05315_a6');
CREATE TABLE 05315_a6 ENGINE = Alias(currentDatabase(), '05315_a7');
CREATE TABLE 05315_a7 ENGINE = Alias(currentDatabase(), '05315_a8');
CREATE TABLE 05315_a8 ENGINE = Alias(currentDatabase(), '05315_a9');
CREATE TABLE 05315_a9 ENGINE = Alias(currentDatabase(), '05315_a10');
CREATE TABLE 05315_a10 ENGINE = Alias(currentDatabase(), '05315_a11');
CREATE TABLE 05315_a11 ENGINE = Alias(currentDatabase(), '05315_a12');
CREATE TABLE 05315_a12 ENGINE = Alias(currentDatabase(), '05315_a13');
CREATE TABLE 05315_a13 ENGINE = Alias(currentDatabase(), '05315_a14');
CREATE TABLE 05315_a14 ENGINE = Alias(currentDatabase(), '05315_a15');
CREATE TABLE 05315_a15 ENGINE = Alias(currentDatabase(), '05315_a16');
CREATE TABLE 05315_a16 ENGINE = Alias(currentDatabase(), '05315_a17');
CREATE TABLE 05315_a17 ENGINE = Alias(currentDatabase(), '05315_a18');
CREATE TABLE 05315_a18 ENGINE = Alias(currentDatabase(), '05315_a19');
CREATE TABLE 05315_a19 ENGINE = Alias(currentDatabase(), '05315_a20');
CREATE TABLE 05315_a20 ENGINE = Alias(currentDatabase(), '05315_a21');
CREATE TABLE 05315_a21 ENGINE = Alias(currentDatabase(), '05315_a22');
CREATE TABLE 05315_a22 ENGINE = Alias(currentDatabase(), '05315_a23');
CREATE TABLE 05315_a23 ENGINE = Alias(currentDatabase(), '05315_a24');
CREATE TABLE 05315_a24 ENGINE = Alias(currentDatabase(), '05315_a25');
CREATE TABLE 05315_a25 ENGINE = Alias(currentDatabase(), '05315_a26');
CREATE TABLE 05315_a26 ENGINE = Alias(currentDatabase(), '05315_a27');
CREATE TABLE 05315_a27 ENGINE = Alias(currentDatabase(), '05315_a28');
CREATE TABLE 05315_a28 ENGINE = Alias(currentDatabase(), '05315_a29');
CREATE TABLE 05315_a29 ENGINE = Alias(currentDatabase(), '05315_a30');
CREATE TABLE 05315_a30 ENGINE = Alias(currentDatabase(), '05315_a31');
CREATE TABLE 05315_a31 ENGINE = Alias(currentDatabase(), '05315_a32');
CREATE TABLE 05315_a32 ENGINE = Alias(currentDatabase(), '05315_a33');
CREATE TABLE 05315_a33 ENGINE = Alias(currentDatabase(), '05315_a34');
CREATE TABLE 05315_a34 ENGINE = Alias(currentDatabase(), '05315_a35');
CREATE TABLE 05315_a35 ENGINE = Alias(currentDatabase(), '05315_a36');
CREATE TABLE 05315_a36 ENGINE = Alias(currentDatabase(), '05315_a37');
CREATE TABLE 05315_a37 ENGINE = Alias(currentDatabase(), '05315_a38');
CREATE TABLE 05315_a38 ENGINE = Alias(currentDatabase(), '05315_a39');
CREATE TABLE 05315_a39 ENGINE = Alias(currentDatabase(), '05315_a40');
CREATE TABLE 05315_a40 ENGINE = Alias(currentDatabase(), '05315_a41');
CREATE TABLE 05315_a41 ENGINE = Alias(currentDatabase(), '05315_a42');
CREATE TABLE 05315_a42 ENGINE = Alias(currentDatabase(), '05315_a43');
CREATE TABLE 05315_a43 ENGINE = Alias(currentDatabase(), '05315_a44');
CREATE TABLE 05315_a44 ENGINE = Alias(currentDatabase(), '05315_a45');
CREATE TABLE 05315_a45 ENGINE = Alias(currentDatabase(), '05315_a46');
CREATE TABLE 05315_a46 ENGINE = Alias(currentDatabase(), '05315_a47');
CREATE TABLE 05315_a47 ENGINE = Alias(currentDatabase(), '05315_a48');
CREATE TABLE 05315_a48 ENGINE = Alias(currentDatabase(), '05315_a49');
CREATE TABLE 05315_a49 ENGINE = Alias(currentDatabase(), '05315_a50');
CREATE TABLE 05315_a50 ENGINE = Alias(currentDatabase(), '05315_a51');
CREATE TABLE 05315_a51 ENGINE = Alias(currentDatabase(), '05315_a52');
CREATE TABLE 05315_a52 ENGINE = Alias(currentDatabase(), '05315_a53');
CREATE TABLE 05315_a53 ENGINE = Alias(currentDatabase(), '05315_a54');
CREATE TABLE 05315_a54 ENGINE = Alias(currentDatabase(), '05315_a55');
CREATE TABLE 05315_a55 ENGINE = Alias(currentDatabase(), '05315_a56');
CREATE TABLE 05315_a56 ENGINE = Alias(currentDatabase(), '05315_a57');
CREATE TABLE 05315_a57 ENGINE = Alias(currentDatabase(), '05315_a58');
CREATE TABLE 05315_a58 ENGINE = Alias(currentDatabase(), '05315_a59');
CREATE TABLE 05315_a59 ENGINE = Alias(currentDatabase(), '05315_a60');
CREATE TABLE 05315_a60 ENGINE = Alias(currentDatabase(), '05315_a61');
CREATE TABLE 05315_a61 ENGINE = Alias(currentDatabase(), '05315_a62');
CREATE TABLE 05315_a62 ENGINE = Alias(currentDatabase(), '05315_a63');
CREATE TABLE 05315_a63 ENGINE = Alias(currentDatabase(), '05315_a64');
CREATE TABLE 05315_a64 ENGINE = Alias(currentDatabase(), '05315_a65');
CREATE TABLE 05315_a65 ENGINE = Alias(currentDatabase(), '05315_a66');
CREATE TABLE 05315_a66 ENGINE = Alias(currentDatabase(), '05315_a67');
CREATE TABLE 05315_a67 ENGINE = Alias(currentDatabase(), '05315_a68');
CREATE TABLE 05315_a68 ENGINE = Alias(currentDatabase(), '05315_a69');
CREATE TABLE 05315_a69 ENGINE = Alias(currentDatabase(), '05315_a70');
CREATE TABLE 05315_a70 ENGINE = Alias(currentDatabase(), '05315_pv_declared');

CREATE TABLE 05315_merge (n UInt64) ENGINE = Merge(currentDatabase(), '^05315_a1$');

-- { echoOn }

DESCRIBE TABLE 05315_a1;
SELECT n FROM 05315_merge; -- { serverError STORAGE_REQUIRES_PARAMETER }

-- { echoOff }

DROP TABLE 05315_merge;
DROP TABLE 05315_a1;
DROP TABLE 05315_a2;
DROP TABLE 05315_a3;
DROP TABLE 05315_a4;
DROP TABLE 05315_a5;
DROP TABLE 05315_a6;
DROP TABLE 05315_a7;
DROP TABLE 05315_a8;
DROP TABLE 05315_a9;
DROP TABLE 05315_a10;
DROP TABLE 05315_a11;
DROP TABLE 05315_a12;
DROP TABLE 05315_a13;
DROP TABLE 05315_a14;
DROP TABLE 05315_a15;
DROP TABLE 05315_a16;
DROP TABLE 05315_a17;
DROP TABLE 05315_a18;
DROP TABLE 05315_a19;
DROP TABLE 05315_a20;
DROP TABLE 05315_a21;
DROP TABLE 05315_a22;
DROP TABLE 05315_a23;
DROP TABLE 05315_a24;
DROP TABLE 05315_a25;
DROP TABLE 05315_a26;
DROP TABLE 05315_a27;
DROP TABLE 05315_a28;
DROP TABLE 05315_a29;
DROP TABLE 05315_a30;
DROP TABLE 05315_a31;
DROP TABLE 05315_a32;
DROP TABLE 05315_a33;
DROP TABLE 05315_a34;
DROP TABLE 05315_a35;
DROP TABLE 05315_a36;
DROP TABLE 05315_a37;
DROP TABLE 05315_a38;
DROP TABLE 05315_a39;
DROP TABLE 05315_a40;
DROP TABLE 05315_a41;
DROP TABLE 05315_a42;
DROP TABLE 05315_a43;
DROP TABLE 05315_a44;
DROP TABLE 05315_a45;
DROP TABLE 05315_a46;
DROP TABLE 05315_a47;
DROP TABLE 05315_a48;
DROP TABLE 05315_a49;
DROP TABLE 05315_a50;
DROP TABLE 05315_a51;
DROP TABLE 05315_a52;
DROP TABLE 05315_a53;
DROP TABLE 05315_a54;
DROP TABLE 05315_a55;
DROP TABLE 05315_a56;
DROP TABLE 05315_a57;
DROP TABLE 05315_a58;
DROP TABLE 05315_a59;
DROP TABLE 05315_a60;
DROP TABLE 05315_a61;
DROP TABLE 05315_a62;
DROP TABLE 05315_a63;
DROP TABLE 05315_a64;
DROP TABLE 05315_a65;
DROP TABLE 05315_a66;
DROP TABLE 05315_a67;
DROP TABLE 05315_a68;
DROP TABLE 05315_a69;
DROP TABLE 05315_a70;

-- { echoOn }

-- A latched schema is checked against the output of the substituted `SELECT`, so only ordinary
-- columns can be declared.
CREATE VIEW 05315_pv_alias_column (n UInt64, s String ALIAS toString(n)) AS SELECT number AS n FROM numbers({upper_bound:UInt64}); -- { serverError BAD_ARGUMENTS }
CREATE VIEW 05315_pv_materialized_column (n UInt64, s String MATERIALIZED toString(n)) AS SELECT number AS n FROM numbers({upper_bound:UInt64}); -- { serverError BAD_ARGUMENTS }
CREATE VIEW 05315_pv_ephemeral_column (n UInt64, s String EPHEMERAL) AS SELECT number AS n FROM numbers({upper_bound:UInt64}); -- { serverError BAD_ARGUMENTS }

-- Without the setting the declared list is dropped, so it is not checked either.
SET use_declared_schema_for_parameterized_views = 0;
CREATE VIEW 05315_pv_alias_column (n UInt64, s String ALIAS toString(n)) AS SELECT number AS n FROM numbers({upper_bound:UInt64});
SELECT n FROM 05315_pv_alias_column(upper_bound = 2) ORDER BY n;

-- { echoOff }

DROP VIEW 05315_pv_alias_column;
DROP VIEW 05315_pv_declared;
DROP VIEW 05315_pv_no_schema;
