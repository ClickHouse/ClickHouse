SELECT t.t.t.* FROM system.tables WHERE database = currentDatabase(); --{serverError INVALID_IDENTIFIER}
