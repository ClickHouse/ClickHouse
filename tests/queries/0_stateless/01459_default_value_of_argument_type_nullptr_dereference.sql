SELECT defaultValueOfTypeName(materialize(FQDN())); -- { serverError ILLEGAL_COLUMN }
