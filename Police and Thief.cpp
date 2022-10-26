#include <iostream>
#include<math.h>
using namespace std;
int main() {
	 int t,x,y;
	 cin>>t;
	 for(int i=1; i<=t; i++){
	     cin>>x>>y;
	     int ans=abs(x-y);
	     cout<<ans<<endl;
	 }
	return 0;
}
