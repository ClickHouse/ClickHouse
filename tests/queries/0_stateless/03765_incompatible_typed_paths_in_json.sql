select '{}'::JSON(a UInt32, a.b UInt32); -- {serverError BAD_ARGUMENTS}
select '{}'::JSON(a.b UInt32, a UInt32); -- {serverError BAD_ARGUMENTS}

