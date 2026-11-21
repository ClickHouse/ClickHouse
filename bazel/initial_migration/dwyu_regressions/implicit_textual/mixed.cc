#include <dwyu_alias/sample/api.h>
#define DWYU_ALIAS_EXPLICIT_FIELD(field_type, field_name) field_type field_name;
struct DwyuAliasMixed {
#include "bazel/initial_migration/dwyu_regressions/implicit_textual/explicit_textual.h"
};
#undef DWYU_ALIAS_EXPLICIT_FIELD
int dwyu_alias_mixed(DwyuAliasPublic first, DwyuAliasMixed second) {
    return first.member.value + second.extra;
}
