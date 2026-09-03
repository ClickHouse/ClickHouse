set compatibility='a.a'; -- { serverError BAD_ARGUMENTS }
select value, changed from system.settings where name = 'compatibility'

