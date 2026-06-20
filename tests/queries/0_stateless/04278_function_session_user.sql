-- Since the actual user name is unknown, have to perform just smoke tests
SET enable_analyzer = 1;
SELECT SESSION_USER IS NOT NULL;
SELECT length(SESSION_USER) > 0;
SELECT SESSION_USER = currentUser(), SESSION_USER() = currentUser(), session_user() = currentUser();
