export const EnterprisePlanFeatureBadge = ({feature='Esta función', support=false, linking_verb_are = false}) => {
    return (
        <div className="enterprisePlanFeatureContainer">
            <div className="enterprisePlanFeatureBadge">
                Función del plan Enterprise
            </div>
            <div>
                <p>{feature} {linking_verb_are ? 'están' : 'está'} {linking_verb_are ? 'disponibles' : 'disponible'} en el plan Enterprise. {support ? `Contacte con soporte para habilitar esta función.` : 'Para cambiar de plan, visite la página de planes en la consola de Cloud.'}</p>
            </div>
        </div>
    )
}