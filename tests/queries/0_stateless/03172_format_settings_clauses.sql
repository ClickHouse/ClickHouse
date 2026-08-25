SET max_block_size = 10, max_threads = 1;

-- Take the following example:
SELECT 1 UNION ALL SELECT 2 FORMAT TSV;

-- Each subquery can be put in parentheses and have its own settings:
(SELECT getSetting('max_block_size') SETTINGS max_block_size = 1) UNION ALL (SELECT getSetting('max_block_size') SETTINGS max_block_size = 2) FORMAT TSV;

-- And the whole query can have settings:
(SELECT getSetting('max_block_size') SETTINGS max_block_size = 1) UNION ALL (SELECT getSetting('max_block_size') SETTINGS max_block_size = 2) FORMAT TSV SETTINGS max_block_size = 3;

-- A single query with output is parsed in the same way as the UNION ALL chain:
SELECT getSetting('max_block_size') SETTINGS max_block_size = 1 FORMAT TSV SETTINGS max_block_size = 3;

-- So while these forms have a slightly different meaning, they both exist:
SELECT getSetting('max_block_size') SETTINGS max_block_size = 1 FORMAT TSV;
SELECT getSetting('max_block_size') FORMAT TSV SETTINGS max_block_size = 3;

-- And due to this effect, the users expect that the FORMAT and SETTINGS may go in an arbitrary order.
-- But while this work:
(SELECT getSetting('max_block_size')) UNION ALL (SELECT getSetting('max_block_size')) FORMAT TSV SETTINGS max_block_size = 3;

-- This does not work automatically, unless we explicitly allow different orders:
(SELECT getSetting('max_block_size')) UNION ALL (SELECT getSetting('max_block_size')) SETTINGS max_block_size = 3 FORMAT TSV;

-- Inevitably, we allow this:
SELECT getSetting('max_block_size') SETTINGS max_block_size = 1 SETTINGS max_block_size = 3 FORMAT TSV;
/*^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^*/
-- Because this part is consumed into ASTSelectWithUnionQuery
-- and the rest into ASTQueryWithOutput.
