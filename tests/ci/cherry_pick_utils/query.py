# -*- coding: utf-8 -*-

import requests
import time


class Query:
    '''
    Implements queries to the Github API using GraphQL
    '''

    _PULL_REQUEST = '''
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
    '''

    def __init__(self, token, owner, name, team, max_page_size=100, min_page_size=10):
        self._PULL_REQUEST = Query._PULL_REQUEST.format(min_page_size=min_page_size)

        self._token = token
        self._owner = owner
        self._name = name
        self._team = team

        self._max_page_size = max_page_size
        self._min_page_size = min_page_size

        self.api_costs = {}

        repo = self.get_repository()
        self._id = repo['id']
        self.ssh_url = repo['sshUrl']
        self.default_branch = repo['defaultBranchRef']['name']

        self.members = set(self.get_members())

    def get_repository(self):
        _QUERY = '''
            repository(owner: "{owner}" name: "{name}") {{
                defaultBranchRef {{
                    name
                }}
                id
                sshUrl
            }}
        '''

        query = _QUERY.format(owner=self._owner, name=self._name)
        return self._run(query)['repository']

    def get_members(self):
        '''Get all team members for organization

        Returns:
            members: a map of members' logins to ids
        '''

        _QUERY = '''
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
        '''

        members = {}
        not_end = True
        query = _QUERY.format(organization=self._owner, team=self._team,
                              max_page_size=self._max_page_size,
                              next='')

        while not_end:
            result = self._run(query)['organization']['team']
            if result is None:
                break
            result = result['members']
            not_end = result['pageInfo']['hasNextPage']
            query = _QUERY.format(organization=self._owner, team=self._team,
                                  max_page_size=self._max_page_size,
                                  next='after: "{}"'.format(result["pageInfo"]["endCursor"]))

            members += dict([(node['login'], node['id']) for node in result['nodes']])

        return members

    def get_pull_request(self, number):
        _QUERY = '''
            repository(owner: "{owner}" name: "{name}") {{
                pullRequest(number: {number}) {{
                    {pull_request_data}
                }}
            }}
        '''

        query = _QUERY.format(owner=self._owner, name=self._name, number=number,
                              pull_request_data=self._PULL_REQUEST, min_page_size=self._min_page_size)
        return self._run(query)['repository']['pullRequest']

    def find_pull_request(self, base, head):
        _QUERY = '''
            repository(owner: "{owner}" name: "{name}") {{
                pullRequests(first: {min_page_size} baseRefName: "{base}" headRefName: "{head}") {{
                    nodes {{
                        {pull_request_data}
                    }}
                    totalCount
                }}
            }}
        '''

        query = _QUERY.format(owner=self._owner, name=self._name, base=base, head=head,
                              pull_request_data=self._PULL_REQUEST, min_page_size=self._min_page_size)
        result = self._run(query)['repository']['pullRequests']
        if result['totalCount'] > 0:
            return result['nodes'][0]
        else:
            return {}

    def find_pull_requests(self, label_name):
        '''
        Get all pull-requests filtered by label name
        '''
        _QUERY = '''
            repository(owner: "{owner}" name: "{name}") {{
                pullRequests(first: {min_page_size} labels: "{label_name}" states: OPEN) {{
                    nodes {{
                        {pull_request_data}
                    }}
                }}
            }}
        '''

        query = _QUERY.format(owner=self._owner, name=self._name, label_name=label_name,
                              pull_request_data=self._PULL_REQUEST, min_page_size=self._min_page_size)
        return self._run(query)['repository']['pullRequests']['nodes']

    def get_pull_requests(self, before_commit):
        '''
        Get all merged pull-requests from the HEAD of default branch to the last commit (excluding)
        '''

        _QUERY = '''
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
        '''

        pull_requests = []
        not_end = True
        query = _QUERY.format(owner=self._owner, name=self._name,
                              max_page_size=self._max_page_size,
                              min_page_size=self._min_page_size,
                              pull_request_data=self._PULL_REQUEST,
                              next='')

        while not_end:
            result = self._run(query)['repository']['defaultBranchRef']['target']['history']
            not_end = result['pageInfo']['hasNextPage']
            query = _QUERY.format(owner=self._owner, name=self._name,
                                  max_page_size=self._max_page_size,
                                  min_page_size=self._min_page_size,
                                  pull_request_data=self._PULL_REQUEST,
                                  next='after: "{}"'.format(result["pageInfo"]["endCursor"]))

            for commit in result['nodes']:
                # FIXME: maybe include `before_commit`?
                if str(commit['oid']) == str(before_commit):
                    not_end = False
                    break

                # TODO: fetch all pull-requests that were merged in a single commit.
                assert commit['associatedPullRequests']['totalCount'] <= self._min_page_size

                for pull_request in commit['associatedPullRequests']['nodes']:
                    if(pull_request['baseRepository']['nameWithOwner'] == '{}/{}'.format(self._owner, self._name) and
                       pull_request['baseRefName'] == self.default_branch and
                       pull_request['mergeCommit']['oid'] == commit['oid']):
                        pull_requests.append(pull_request)

        return pull_requests

    def create_pull_request(self, source, target, title, description="", draft=False, can_modify=True):
        _QUERY = '''
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
        '''

        query = _QUERY.format(target=target, source=source, id=self._id, title=title, body=description,
                              draft="true" if draft else "false", modify="true" if can_modify else "false",
                              pull_request_data=self._PULL_REQUEST)
        return self._run(query, is_mutation=True)['createPullRequest']['pullRequest']

    def merge_pull_request(self, id):
        _QUERY = '''
            mergePullRequest(input: {{
                pullRequestId: "{id}"
            }}) {{
                pullRequest {{
                    {pull_request_data}
                }}
            }}
        '''

        query = _QUERY.format(id=id, pull_request_data=self._PULL_REQUEST)
        return self._run(query, is_mutation=True)['mergePullRequest']['pullRequest']

    # FIXME: figure out how to add more assignees at once
    def add_assignee(self, pr, assignee):
        _QUERY = '''
            addAssigneesToAssignable(input: {{
                assignableId: "{id1}",
                assigneeIds: "{id2}"
            }}) {{
                clientMutationId
            }}
        '''

        query = _QUERY.format(id1=pr['id'], id2=assignee['id'])
        self._run(query, is_mutation=True)

    def set_label(self, pull_request, label_name):
        '''
        Set label by name to the pull request

        Args:
            pull_request: JSON object returned by `get_pull_requests()`
            label_name (string): label name
        '''

        _GET_LABEL = '''
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
        '''

        _SET_LABEL = '''
            addLabelsToLabelable(input: {{
                labelableId: "{pr_id}",
                labelIds: "{label_id}"
            }}) {{
                clientMutationId
            }}
        '''

        labels = []
        not_end = True
        query = _GET_LABEL.format(owner=self._owner, name=self._name, label_name=label_name,
                                  max_page_size=self._max_page_size,
                                  next='')

        while not_end:
            result = self._run(query)['repository']['labels']
            not_end = result['pageInfo']['hasNextPage']
            query = _GET_LABEL.format(owner=self._owner, name=self._name, label_name=label_name,
                                      max_page_size=self._max_page_size,
                                      next='after: "{}"'.format(result["pageInfo"]["endCursor"]))

            labels += [label for label in result['nodes']]

        if not labels:
            return

        query = _SET_LABEL.format(pr_id=pull_request['id'], label_id=labels[0]['id'])
        self._run(query, is_mutation=True)

    def _run(self, query, is_mutation=False):
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        # sleep a little, because we querying github too often
        print("Request, is mutation", is_mutation)
        time.sleep(0.5)

        def requests_retry_session(
            retries=5,
            backoff_factor=0.5,
            status_forcelist=(403, 500, 502, 504),
            session=None,
        ):
            session = session or requests.Session()
            retry = Retry(
                total=retries,
                read=retries,
                connect=retries,
                backoff_factor=backoff_factor,
                status_forcelist=status_forcelist,
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            return session

        headers = {'Authorization': 'bearer {}'.format(self._token)}
        if is_mutation:
            query = '''
            mutation {{
                {query}
            }}
            '''.format(query=query)
        else:
            query = '''
            query {{
                {query}
                rateLimit {{
                    cost
                    remaining
                }}
            }}
            '''.format(query=query)

        while True:
            request = requests_retry_session().post('https://api.github.com/graphql', json={'query': query}, headers=headers)
            if request.status_code == 200:
                result = request.json()
                if 'errors' in result:
                    raise Exception('Errors occurred: {}\nOriginal query: {}'.format(result["errors"], query))

                if not is_mutation:
                    import inspect
                    caller = inspect.getouterframes(inspect.currentframe(), 2)[1][3]
                    if caller not in list(self.api_costs.keys()):
                        self.api_costs[caller] = 0
                    self.api_costs[caller] += result['data']['rateLimit']['cost']

                return result['data']
            else:
                import json
                raise Exception('Query failed with code {code}:\n{json}'.format(code=request.status_code, json=json.dumps(request.json(), indent=4)))
