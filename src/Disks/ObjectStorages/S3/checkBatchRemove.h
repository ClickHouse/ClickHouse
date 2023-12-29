#pragma once
#include <string>

namespace DB
{

class S3ObjectStorage;

bool checkBatchRemove(S3ObjectStorage & storage, const std::string & key_with_trailing_slash);

}
