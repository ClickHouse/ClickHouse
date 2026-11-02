-- A projection text index is analyzed and read under the projection's name, in the same namespace as the secondary
-- indexes of the table. Sharing a name with a secondary index is therefore rejected.

SET allow_experimental_projection_text_index = 1;
SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab_name_clash;

CREATE TABLE tab_name_clash
(
    id UInt64,
    s String,
    t String,
    INDEX idx s TYPE minmax,
    PROJECTION idx INDEX t TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id; -- { serverError BAD_ARGUMENTS }

CREATE TABLE tab_name_clash
(
    id UInt64,
    s String,
    t String,
    PROJECTION idx INDEX t TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY id;

ALTER TABLE tab_name_clash ADD INDEX idx s TYPE minmax; -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_name_clash ADD INDEX idx_minmax s TYPE minmax;
ALTER TABLE tab_name_clash ADD PROJECTION idx_minmax INDEX s TYPE text(tokenizer = 'splitByNonAlpha'); -- { serverError BAD_ARGUMENTS }
ALTER TABLE tab_name_clash ADD PROJECTION idx_text INDEX s TYPE text(tokenizer = 'splitByNonAlpha');

-- Distinct names: both kinds of index coexist, and the projection text index still answers queries.
INSERT INTO tab_name_clash VALUES (1, 'alpha beta', 'gamma delta'), (2, 'epsilon zeta', 'eta theta');
SELECT id FROM tab_name_clash WHERE hasToken(t, 'eta') ORDER BY id;
SELECT id FROM tab_name_clash WHERE hasToken(s, 'zeta') ORDER BY id;

SELECT name FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 'tab_name_clash' ORDER BY name;
SELECT name FROM system.projections WHERE database = currentDatabase() AND table = 'tab_name_clash' ORDER BY name;

DROP TABLE tab_name_clash;
