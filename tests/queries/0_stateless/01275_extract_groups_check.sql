SELECT extractGroups('hello', ''); -- { serverError BAD_ARGUMENTS }
SELECT extractAllGroups('hello', ''); -- { serverError BAD_ARGUMENTS }

SELECT extractGroups('hello', ' '); -- { serverError BAD_ARGUMENTS }
SELECT extractAllGroups('hello', ' '); -- { serverError BAD_ARGUMENTS }

SELECT extractGroups('hello', '\0'); -- { serverError BAD_ARGUMENTS }
SELECT extractAllGroups('hello', '\0'); -- { serverError BAD_ARGUMENTS }

SELECT extractGroups('hello', 'world'); -- { serverError BAD_ARGUMENTS }
SELECT extractAllGroups('hello', 'world'); -- { serverError BAD_ARGUMENTS }

SELECT extractGroups('hello', 'hello|world'); -- { serverError BAD_ARGUMENTS }
SELECT extractAllGroups('hello', 'hello|world'); -- { serverError BAD_ARGUMENTS }
