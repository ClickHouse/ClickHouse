SELECT count() FROM system.parts WHERE table = NULL AND database = currentDatabase();
SELECT DISTINCT marks FROM system.parts WHERE (table = NULL) AND (database = currentDatabase()) AND (active = 1);
