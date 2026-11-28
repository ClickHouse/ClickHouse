-- `uuid_type_version` decides which concrete type a bare `UUID` is persisted as, so an unsupported value has to
-- be rejected instead of silently behaving like version 1 and creating a table with the historical type.

SET uuid_type_version = 1;
SELECT getSetting('uuid_type_version');
SET uuid_type_version = 2;
SELECT getSetting('uuid_type_version');

SET uuid_type_version = 0; -- { serverError BAD_ARGUMENTS }
SET uuid_type_version = 3; -- { serverError BAD_ARGUMENTS }
SET uuid_type_version = 42; -- { serverError BAD_ARGUMENTS }

-- A rejected `SET` leaves the previous value in place.
SELECT getSetting('uuid_type_version');
