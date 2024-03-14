SELECT match('', repeat('(', 100000)); -- { serverError 306 }
