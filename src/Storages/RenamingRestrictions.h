#pragma once

namespace DB
{

enum RenamingRestrictions
{
    ALLOW_ANY,
    ALLOW_PRESERVING_UUID,
    DO_NOT_ALLOW,
};

}
