SELECT t.t.t.* FROM system.tables WHERE database = currentDatabase(); --{serverError UNKNOWN_IDENTIFIER}
