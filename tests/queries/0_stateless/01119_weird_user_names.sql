
drop user if exists "       ";
drop user if exists '   spaces';
drop user if exists 'spaces    ';
drop user if exists " spaces ";
drop user if exists "test 01119";
drop user if exists "Вася Пупкин";
drop user if exists "无名氏 ";
drop user if exists "🙈 🙉 🙊";

create user "       ";
create user '   spaces';
create user 'spaces    ';
create user ` INTERSERVER SECRET `;  -- { serverError BAD_ARGUMENTS }
create user '';  -- { clientError SYNTAX_ERROR }
create user 'test 01119';
alter user `test 01119` rename to " spaces ";
alter user " spaces " rename to '';  -- { clientError SYNTAX_ERROR }
alter user " spaces " rename to " INTERSERVER SECRET ";  -- { serverError BAD_ARGUMENTS }
create user "Вася Пупкин";
create user "无名氏 ";
create user "🙈 🙉 🙊";

select length(name), name, '.' from system.users where position(name, ' ')!=0 order by name;

drop user "       ";
drop user '   spaces';
drop user 'spaces    ';
drop user " spaces ";
drop user "Вася Пупкин";
drop user "无名氏 ";
drop user "🙈 🙉 🙊";
