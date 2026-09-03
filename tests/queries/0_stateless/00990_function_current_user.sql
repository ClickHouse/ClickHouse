-- Since the actual user name is unknown, have to perform just smoke tests
select currentUser() IS NOT NULL;
select length(currentUser()) > 0;
select currentUser() = user(), currentUser() = USER(), current_user() = currentUser();
select currentUser() = initial_user from system.processes where query like '%$!@#%' AND current_database = currentDatabase();
