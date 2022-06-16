# -*- coding: utf-8 -*-

import json
import inspect
import logging
import time
from urllib3.util.retry import Retry  # type: ignore

import requests  # type: ignore
from requests.adapters import HTTPAdapter  # type: ignore


class Query:
    """
    Implements queries to the Github API using GraphQL
    """

    _PULL_REQUEST = """
author {{
    ... on User {{
        id
        login
    }}
}}

baseRepository {{
    nameWithOwner
}}

mergeCommit {{
    oid
    parents(first: {min_page_size}) {{
        totalCount
        nodes {{
            oid
        }}
    }}
}}

mergedBy {{
    ... on User {{
        id
        login
    }}
}}

baseRefName
closed
headRefName
id
mergeable
merged
number
title
url
    """

    def __init__(self, token, owner, name, team, max_page_size=100, min_page_size=10):
        self._PULL_REQUEST = Query._PULL_REQUEST.format(min_page_size=min_page_size)

        self._token = token
        self._owner = owner
        self._name = name
        self._team = team
        self._session = None

        self._max_page_size = max_page_size
        self._min_page_size = min_page_size

        self.api_costs = {}

        repo = self.get_repository()
        self._id = repo["id"]
        self.ssh_url = repo["sshUrl"]
        self.default_branch = repo["defaultBranchRef"]["name"]

        self.members = set(self.get_members())

    def get_repository(self):
        _QUERY = """
repository(owner: "{owner}" name: "{name}") {{
    defaultBranchRef {{
        name
    }}
    id
    sshUrl
}}
        """

        query = _QUERY.format(owner=self._owner, name=self._name)
        return self._run(query)["repository"]

    def get_members(self):
        """Get all team members for organization

        Returns:
            members: a map of members' logins to ids
        """

        _QUERY = """
organization(login: "{organization}") {{
    team(slug: "{team}") {{
        members(first: {max_page_size} {next}) {{
            pageInfo {{
                hasNextPage
                endCursor
            }}
            nodes {{
                id
                login
            }}
        }}
    }}
}}
        """

        members = {}
        not_end = True
        query = _QUERY.format(
            organization=self._owner,
            team=self._team,
            max_page_size=self._max_page_size,
            next="",
        )

        while not_end:
            result = self._run(query)["organization"]["team"]
            if result is None:
                break
            result = result["members"]
            not_end = result["pageInfo"]["hasNextPage"]
            query = _QUERY.format(
                organization=self._owner,
                team=self._team,
                max_page_size=self._max_page_size,
                next=f'after: "{result["pageInfo"]["endCursor"]}"',
            )

            # Update members with new nodes compatible with py3.8-py3.10
            members = {
                **members,
                **{node["login"]: node["id"] for node in result["nodes"]},
            }

        return members

    def get_pull_request(self, number):
        _QUERY = """
repository(owner: "{owner}" name: "{name}") {{
    pullRequest(number: {number}) {{
        {pull_request_data}
    }}
}}
        """

        query = _QUERY.format(
            owner=self._owner,
            name=self._name,
            number=number,
            pull_request_data=self._PULL_REQUEST,
            min_page_size=self._min_page_size,
        )
        return self._run(query)["repository"]["pullRequest"]

    def find_pull_request(self, base, head):
        _QUERY = """
repository(owner: "{owner}" name: "{name}") {{
    pullRequests(
            first: {min_page_size} baseRefName: "{base}" headRefName: "{head}"
    ) {{
        nodes {{
            {pull_request_data}
        }}
        totalCount
    }}
}}
        """

        query = _QUERY.format(
            owner=self._owner,
            name=self._name,
            base=base,
            head=head,
            pull_request_data=self._PULL_REQUEST,
            min_page_size=self._min_page_size,
        )
        result = self._run(query)["repository"]["pullRequests"]
        if result["totalCount"] > 0:
            return result["nodes"][0]
        else:
            return {}

    def find_pull_requests(self, label_name):
        """
        Get all pull-requests filtered by label name
        """
        _QUERY = """
repository(owner: "{owner}" name: "{name}") {{
    pullRequests(first: {min_page_size} labels: "{label_name}" states: OPEN) {{
        nodes {{
            {pull_request_data}
        }}
    }}
}}
        """

        query = _QUERY.format(
            owner=self._owner,
            name=self._name,
            label_name=label_name,
            pull_request_data=self._PULL_REQUEST,
            min_page_size=self._min_page_size,
        )
        return self._run(query)["repository"]["pullRequests"]["nodes"]

    def get_pull_requests(self, before_commit):
        """
        Get all merged pull-requests from the HEAD of default branch to the last commit (excluding)
        """

        _QUERY = """
repository(owner: "{owner}" name: "{name}") {{
    defaultBranchRef {{
        target {{
            ... on Commit {{
                history(first: {max_page_size} {next}) {{
                    pageInfo {{
                        hasNextPage
                        endCursor
                    }}
                    nodes {{
                        oid
                        associatedPullRequests(first: {min_page_size}) {{
                            totalCount
                            nodes {{
                                ... on PullRequest {{
                                    {pull_request_data}

                                    labels(first: {min_page_size}) {{
                                        totalCount
                                        pageInfo {{
                                            hasNextPage
                                            endCursor
                                        }}
                                        nodes {{
                                            name
                                            color
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        }}
    }}
}}
        """

        pull_requests = []
        not_end = True
        query = _QUERY.format(
            owner=self._owner,
            name=self._name,
            max_page_size=self._max_page_size,
            min_page_size=self._min_page_size,
            pull_request_data=self._PULL_REQUEST,
            next="",
        )

        while not_end:
            result = self._run(query)["repository"]["defaultBranchRef"]["target"][
                "history"
            ]
            not_end = result["pageInfo"]["hasNextPage"]
            query = _QUERY.format(
                owner=self._owner,
                name=self._name,
                max_page_size=self._max_page_size,
                min_page_size=self._min_page_size,
                pull_request_data=self._PULL_REQUEST,
                next=f'after: "{result["pageInfo"]["endCursor"]}"',
            )

            for commit in result["nodes"]:
                # FIXME: maybe include `before_commit`?
                if str(commit["oid"]) == str(before_commit):
                    not_end = False
                    break

                # TODO: fetch all pull-requests that were merged in a single commit.
                assert (
                    commit["associatedPullRequests"]["totalCount"]
                    <= self._min_page_size
                )

                for pull_request in commit["associatedPullRequests"]["nodes"]:
                    if (
                        pull_request["baseRepository"]["nameWithOwner"]
                        == f"{self._owner}/{self._name}"
                        and pull_request["baseRefName"] == self.default_branch
                        and pull_request["mergeCommit"]["oid"] == commit["oid"]
                    ):
                        pull_requests.append(pull_request)

        return pull_requests

    def create_pull_request(
        self, source, target, title, description="", draft=False, can_modify=True
    ):
        _QUERY = """
createPullRequest(input: {{
    baseRefName: "{target}",
    headRefName: "{source}",
    repositoryId: "{id}",
    title: "{title}",
    body: "{body}",
    draft: {draft},
    maintainerCanModify: {modify}
}}) {{
    pullRequest {{
        {pull_request_data}
    }}
}}
        """

        query = _QUERY.format(
            target=target,
            source=source,
            id=self._id,
            title=title,
            body=description,
            draft="true" if draft else "false",
            modify="true" if can_modify else "false",
            pull_request_data=self._PULL_REQUEST,
        )
        return self._run(query, is_mutation=True)["createPullRequest"]["pullRequest"]

    def merge_pull_request(self, pr_id):
        _QUERY = """
mergePullRequest(input: {{
    pullRequestId: "{pr_id}"
}}) {{
    pullRequest {{
        {pull_request_data}
    }}
}}
        """

        query = _QUERY.format(pr_id=pr_id, pull_request_data=self._PULL_REQUEST)
        return self._run(query, is_mutation=True)["mergePullRequest"]["pullRequest"]

    # FIXME: figure out how to add more assignees at once
    def add_assignee(self, pr, assignee):
        _QUERY = """
addAssigneesToAssignable(input: {{
    assignableId: "{id1}",
    assigneeIds: "{id2}"
}}) {{
    clientMutationId
}}
        """

        query = _QUERY.format(id1=pr["id"], id2=assignee["id"])
        self._run(query, is_mutation=True)

    def set_label(self, pull_request, label_name):
        """
        Set label by name to the pull request

        Args:
            pull_request: JSON object returned by `get_pull_requests()`
            label_name (string): label name
        """

        _GET_LABEL = """
repository(owner: "{owner}" name: "{name}") {{
    labels(first: {max_page_size} {next} query: "{label_name}") {{
        pageInfo {{
            hasNextPage
            endCursor
        }}
        nodes {{
            id
            name
            color
        }}
    }}
}}
        """

        _SET_LABEL = """
addLabelsToLabelable(input: {{
    labelableId: "{pr_id}",
    labelIds: "{label_id}"
}}) {{
    clientMutationId
}}
        """

        labels = []
        not_end = True
        query = _GET_LABEL.format(
            owner=self._owner,
            name=self._name,
            label_name=label_name,
            max_page_size=self._max_page_size,
            next="",
        )

        while not_end:
            result = self._run(query)["repository"]["labels"]
            not_end = result["pageInfo"]["hasNextPage"]
            query = _GET_LABEL.format(
                owner=self._owner,
                name=self._name,
                label_name=label_name,
                max_page_size=self._max_page_size,
                next=f'after: "{result["pageInfo"]["endCursor"]}"',
            )

            labels += list(result["nodes"])

        if not labels:
            return

        query = _SET_LABEL.format(pr_id=pull_request["id"], label_id=labels[0]["id"])
        self._run(query, is_mutation=True)

    @property
    def session(self):
        if self._session is not None:
            return self._session
        retries = 5
        self._session = requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=1,
            status_forcelist=(403, 500, 502, 504),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)
        return self._session

    def _run(self, query, is_mutation=False):
        # Get caller and parameters from the stack to track the progress
        frame = inspect.getouterframes(inspect.currentframe(), 2)[1]
        caller = frame[3]
        f_parameters = inspect.signature(getattr(self, caller)).parameters
        parameters = ", ".join(str(frame[0].f_locals[p]) for p in f_parameters)
        mutation = ""
        if is_mutation:
            mutation = ", is mutation"
        print(f"---GraphQL request for {caller}({parameters}){mutation}---")

        headers = {"Authorization": f"bearer {self._token}"}
        if is_mutation:
            query = f"""
mutation {{
    {query}
}}
            """
        else:
            query = f"""
query {{
    {query}
    rateLimit {{
        cost
        remaining
    }}
}}
            """

        def request_with_retry(retry=0):
            max_retries = 5
            # From time to time we face some concrete errors, when it worth to
            # retry instead of failing competely
            # We should sleep progressively
            progressive_sleep = 5 * sum(i + 1 for i in range(retry))
            if progressive_sleep:
                logging.warning(
                    "Retry GraphQL request %s time, sleep %s seconds",
                    retry,
                    progressive_sleep,
                )
                time.sleep(progressive_sleep)
            response = self.session.post(
                "https://api.github.com/graphql", json={"query": query}, headers=headers
            )
            result = response.json()
            if response.status_code == 200:
                if "errors" in result:
                    raise Exception(
                        f"Errors occurred: {result['errors']}\nOriginal query: {query}"
                    )

                if not is_mutation:
                    if caller not in self.api_costs:
                        self.api_costs[caller] = 0
                    self.api_costs[caller] += result["data"]["rateLimit"]["cost"]

                return result["data"]
            elif (
                response.status_code == 403
                and "secondary rate limit" in result["message"]
            ):
                if retry <= max_retries:
                    logging.warning("Secondary rate limit reached")
                    return request_with_retry(retry + 1)
            elif response.status_code == 502 and "errors" in result:
                too_many_data = any(
                    True
                    for err in result["errors"]
                    if "message" in err
                    and "This may be the result of a timeout" in err["message"]
                )
                if too_many_data:
                    logging.warning(
                        "Too many data is requested, decreasing page size %s by 10%%",
                        self._max_page_size,
                    )
                    self._max_page_size = int(self._max_page_size * 0.9)
                    return request_with_retry(retry)

            data = json.dumps(result, indent=4)
            raise Exception(f"Query failed with code {response.status_code}:\n{data}")

        return request_with_retry()
