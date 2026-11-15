export const EnterprisePlanFeatureBadge = ({feature='هذه الميزة', support=false, linking_verb_are = false}) => {
    return (
        <div className="enterprisePlanFeatureContainer">
            <div className="enterprisePlanFeatureBadge">
                ميزة في خطة Enterprise
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'متوفرة' : 'متوفرة'} في خطة Enterprise. {support ? `تواصل مع الدعم لتمكين هذه الميزة.` : 'للترقية، انتقل إلى صفحة الخطط في Cloud Console.'}</p>
            </div>
        </div>
    )
}