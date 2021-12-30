#include <iostream>
#include <string>
#include "MeiliSearchConnection.h"
#include <Common/Exception.h>
#include <base/JSON.h>

int main() {
    String s = "{\"updateId\":4}";
    JSON jres(s);
    std::cout << jres.begin().getName();
    return 0;
}


