from ci.jobs.scripts.docs.generate_cloud_api_reference import (
    localize_fragment,
    operation_ref_keys,
)


def identity_ref_keys(*groups):
    refs = {}

    def collect(group):
        for page in group["pages"]:
            if isinstance(page, str):
                refs[page] = page
            else:
                collect(page)

    for group_list in groups:
        for group in group_list:
            collect(group)
    return refs


def test_localize_fragment_preserves_names_and_syncs_pages():
    generated = [
        {
            "group": "Organization",
            "pages": [
                {"group": "Billing", "pages": ["GET /usage", "POST /invoice"]},
                {"group": "UDF", "pages": ["products/cloud/api-reference/udf/create"]},
            ],
        },
        {
            "group": "ClickStack",
            "pages": ["products/cloud/api-reference/clickstack/list-webhooks"],
        },
    ]
    current = [
        {
            "group": "Organisation traduite",
            "pages": [
                {"group": "Facturation", "pages": ["GET /usage"]},
            ],
        },
        {
            "group": "Pile de clics",
            "pages": [
                "products/cloud/api-reference/clickstack/list-connections",
                "products/cloud/api-reference/clickstack/list-webhooks",
            ],
        },
    ]

    localized = localize_fragment(generated, current, identity_ref_keys(generated, current))

    assert localized == [
        {
            "group": "Organisation traduite",
            "pages": [
                {"group": "Facturation", "pages": ["GET /usage", "POST /invoice"]},
                {"group": "UDF", "pages": ["products/cloud/api-reference/udf/create"]},
            ],
        },
        {
            "group": "Pile de clics",
            "pages": ["products/cloud/api-reference/clickstack/list-webhooks"],
        },
    ]


def test_localize_fragment_matches_reordered_groups_by_pages():
    generated = [
        {"group": "Second", "pages": ["GET /second"]},
        {"group": "First", "pages": ["GET /first"]},
    ]
    current = [
        {"group": "Premier", "pages": ["GET /first"]},
        {"group": "Deuxième", "pages": ["GET /second"]},
    ]

    assert localize_fragment(
        generated, current, identity_ref_keys(generated, current)
    ) == [
        {"group": "Deuxième", "pages": ["GET /second"]},
        {"group": "Premier", "pages": ["GET /first"]},
    ]


def test_localize_fragment_keeps_english_name_when_groups_merge():
    generated = [{"group": "Merged", "pages": ["GET /a", "GET /b"]}]
    current = [
        {"group": "Translated A", "pages": ["GET /a"]},
        {"group": "Translated B", "pages": ["GET /b"]},
    ]

    assert localize_fragment(
        generated, current, identity_ref_keys(generated, current)
    ) == generated


def test_localize_fragment_preserves_names_across_maturity_changes():
    operation = "GET /v1/organizations/{organizationId}/usageCost"
    badge_page = "products/cloud/api-reference/billing/billing-usage-get"
    ref_keys = {operation: operation, badge_page: operation}
    translated = "Organisation traduite"
    translated_billing = "Facturation"

    def fragment(page, organization="Organization", billing="Billing"):
        return [
            {
                "group": organization,
                "pages": [{"group": billing, "pages": [page]}],
            }
        ]

    current_ga = fragment(operation, translated, translated_billing)
    generated_badge = fragment(badge_page)
    assert localize_fragment(generated_badge, current_ga, ref_keys) == fragment(
        badge_page, translated, translated_billing
    )

    current_badge = fragment(badge_page, translated, translated_billing)
    generated_ga = fragment(operation)
    assert localize_fragment(generated_ga, current_badge, ref_keys) == fragment(
        operation, translated, translated_billing
    )


def test_operation_ref_keys_maps_ga_and_badge_forms():
    spec = {
        "paths": {
            "/v1/organizations/{organizationId}/usageCost": {
                "get": {
                    "operationId": "billingUsageGet",
                    "summary": "Get usage cost",
                    "tags": ["Billing"],
                }
            }
        }
    }

    operation = "GET /v1/organizations/{organizationId}/usageCost"
    assert operation_ref_keys(spec) == {
        operation: operation,
        "products/cloud/api-reference/billing/billing-usage-get": operation,
    }
