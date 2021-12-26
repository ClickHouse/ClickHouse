#include <iostream>
#include <string>
#include "MeiliSearchConnection.h"
#include <Common/Exception.h>
#include <base/JSON.h>

int main() {
    String s = R"({"hits":[{"id":"100","title":"Lock, Stock and Two Smoking Barrels"},{"id":"10001","title":"Young Einstein"},{"id":"100017","title":"Hounded"},{"id":"10002","title":"Mona Lisa"},{"id":"10003","title":"The Saint"},{"id":"10004","title":"Desperation"},{"id":"100042","title":"Dumb and Dumber To"},{"id":"100046","title":"The Giant Mechanical Man"},{"id":"10005","title":"Behind Enemy Lines II: Axis of Evil"},{"id":"10007","title":"See No Evil"}],"nbHits":19546,"exhaustiveNbHits":false,"query":"","limit":10,"offset":0,"processingTimeMs":15})";
    JSON jres(s);
    int cnt = 0;
    for (const auto el : jres.begin().getValue()) {
        std::cout << ++cnt << "\n" << el.toString() << "\n";
        String templ = "id";
        String def;
        el.getWithDefault(templ, def);
        for (const auto x : el) {
            // std::cout << x.toString() << "\n";
            std::cout << x.getName() << " | " << x.getValue().toString() << "\n";
        }
    }
    return 0;
}


