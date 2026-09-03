with (select count() > 0 from remote('127.2', system.settings)) as s select s;
-- nested
with (select count() > 0 from remote('127.2', remote('127.2', system.settings))) as s select s;
-- nested via view()
with (select count() > 0 from remote('127.2', view(select count() from remote('127.2', system.settings)))) as s select s;
