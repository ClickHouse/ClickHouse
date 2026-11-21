#pragma once
#define DWYU_ALIAS_FIELD(field_type, field_name) field_type field_name;
struct DwyuAliasPublic {
#include <dwyu_alias/sample/decl.inc>
};
#undef DWYU_ALIAS_FIELD
