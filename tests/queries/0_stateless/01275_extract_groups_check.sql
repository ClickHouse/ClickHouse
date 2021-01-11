SELECT extractGroups('hello', ''); -- { serverError 36 }
SELECT extractAllGroups('hello', ''); -- { serverError 36 }

SELECT extractGroups('hello', ' '); -- { serverError 36 }
SELECT extractAllGroups('hello', ' '); -- { serverError 36 }

SELECT extractGroups('hello', '\0'); -- { serverError 36 }
SELECT extractAllGroups('hello', '\0'); -- { serverError 36 }

SELECT extractGroups('hello', 'world'); -- { serverError 36 }
SELECT extractAllGroups('hello', 'world'); -- { serverError 36 }

SELECT extractGroups('hello', 'hello|world'); -- { serverError 36 }
SELECT extractAllGroups('hello', 'hello|world'); -- { serverError 36 }
