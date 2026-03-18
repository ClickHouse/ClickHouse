#!/usr/bin/env python3

import html
import os
import random
import string

import yaml
from clickhouse_driver import Client

client = Client(host="localhost", port=9000)
settings = {
    "default_table_engine": "Memory",
    "union_default_mode": "DISTINCT",
    "calculate_text_stack_trace": 0,
}

database_name = "sqltest_" + "".join(
    random.choice(string.ascii_lowercase) for _ in range(10)
)

client.execute(f"DROP DATABASE IF EXISTS {database_name}", settings=settings)
client.execute(f"CREATE DATABASE {database_name}", settings=settings)

client = Client(host="localhost", port=9000, database=database_name)

summary = {"success": 0, "total": 0, "results": {}}

log_file = open("test.log", "w")
report_html_file = open("report.html", "w")

with open("features.yml", "r") as file:
    yaml_content = yaml.safe_load(file)

    for category in yaml_content:
        log_file.write(category.capitalize() + " features:\n")
        summary["results"][category] = {"success": 0, "total": 0, "results": {}}

        for test in yaml_content[category]:
            log_file.write(test + ": " + yaml_content[category][test] + "\n")
            summary["results"][category]["results"][test] = {
                "success": 0,
                "total": 0,
                "description": yaml_content[category][test],
            }

            test_path = test[0] + "/" + test + ".tests.yml"
            if os.path.exists(test_path):
                with open(test_path, "r") as test_file:
                    test_yaml_content = yaml.load_all(test_file, Loader=yaml.FullLoader)

                    for test_case in test_yaml_content:
                        queries = test_case["sql"]
                        if not isinstance(queries, list):
                            queries = [queries]

                        for query in queries:
                            # Example: E011-01
                            test_group = ""
                            if "-" in test:
                                test_group = test.split("-", 1)[0]
                                summary["results"][category]["results"][test_group][
                                    "total"
                                ] += 1
                            summary["results"][category]["results"][test]["total"] += 1
                            summary["results"][category]["total"] += 1
                            summary["total"] += 1

                            log_file.write(query + "\n")

                            try:
                                result = client.execute(query, settings=settings)
                                log_file.write(str(result) + "\n")

                                if test_group:
                                    summary["results"][category]["results"][test_group][
                                        "success"
                                    ] += 1
                                summary["results"][category]["results"][test][
                                    "success"
                                ] += 1
                                summary["results"][category]["success"] += 1
                                summary["success"] += 1

                            except Exception as e:
                                log_file.write(f"Error occurred: {str(e)}\n")

client.execute(f"DROP DATABASE {database_name}", settings=settings)


def enable_color(ratio):
    if ratio == 0:
        return "<b style='color: red;'>"
    elif ratio < 0.5:
        return "<b style='color: orange;'>"
    elif ratio < 1:
        return "<b style='color: gray;'>"
    else:
        return "<b style='color: green;'>"


reset_color = "</b>"


def print_ratio(indent, name, success, total, description):
    report_html_file.write(
        "{}{}: {}{} / {} ({:.1%}){}{}\n".format(
            " " * indent,
            name.capitalize(),
            enable_color(success / total),
            success,
            total,
            success / total,
            reset_color,
            f" - " + html.escape(description) if description else "",
        )
    )


report_html_file.write(
    "<html><body><pre style='font-size: 16pt; padding: 1em; line-height: 1.25;'>\n"
)

print_ratio(0, "Total", summary["success"], summary["total"], "")

for category in summary["results"]:
    cat_summary = summary["results"][category]

    if cat_summary["total"] == 0:
        continue

    print_ratio(2, category, cat_summary["success"], cat_summary["total"], "")

    for test in summary["results"][category]["results"]:
        test_summary = summary["results"][category]["results"][test]

        if test_summary["total"] == 0:
            continue

        print_ratio(
            6 if "-" in test else 4,
            test,
            test_summary["success"],
            test_summary["total"],
            test_summary["description"],
        )

report_html_file.write("</pre></body></html>\n")
