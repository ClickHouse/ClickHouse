ATTACH VIEW character_sets
(
    character_set_name String,
    CHARACTER_SET_NAME String
)
SQL SECURITY INVOKER
AS SELECT
    arrayJoin(['utf8', 'utf8mb4', 'ascii', 'binary']) AS character_set_name,
    character_set_name AS CHARACTER_SET_NAME;
