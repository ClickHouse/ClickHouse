SELECT ( SELECT dictGet() ) settings enable_analyzer=0; -- {serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH}

