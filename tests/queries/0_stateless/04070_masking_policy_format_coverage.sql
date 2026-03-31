
-- Coverage for ASTCreateMaskingPolicyQuery::formatImpl (lines 66-108)
-- Exercises: name, ON db.table, IN storage_name, WHERE, PRIORITY, ALTER paths

-- Basic CREATE with database-qualified table
SELECT formatQuery('CREATE MASKING POLICY p ON db.t UPDATE email = \'***\' TO ALL') ORDER BY 1;

-- CREATE without database (just table name)
SELECT formatQuery('CREATE MASKING POLICY p ON t UPDATE email = \'***\' TO ALL') ORDER BY 1;

-- CREATE with IN storage_name
SELECT formatQuery('CREATE MASKING POLICY p ON db.t IN some_storage UPDATE email = \'***\' TO ALL') ORDER BY 1;

-- CREATE with IF NOT EXISTS, WHERE, PRIORITY
SELECT formatQuery('CREATE MASKING POLICY IF NOT EXISTS p ON db.t UPDATE email = \'***\' WHERE id > 0 TO ALL PRIORITY 5') ORDER BY 1;

-- CREATE with OR REPLACE
SELECT formatQuery('CREATE MASKING POLICY OR REPLACE p ON db.t UPDATE email = \'***\' TO ALL') ORDER BY 1;

-- ALTER basic
SELECT formatQuery('ALTER MASKING POLICY IF EXISTS p ON db.t UPDATE email = \'***\'') ORDER BY 1;

-- ALTER with RENAME TO
SELECT formatQuery('ALTER MASKING POLICY p ON db.t RENAME TO q') ORDER BY 1;
